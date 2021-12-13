/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.spark.v2.reader

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.java.util.common.{Intervals, JodaUtils}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.clients.{DruidClient, DruidMetadataClient}
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.utils.{FilterUtils, SchemaUtils}
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition,
  SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsScanColumnarBatch}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.joda.time.Interval

import java.util.{List => JList}
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

/**
  * A DruidDataSourceReader handles the actual work of reading data from Druid. It does this by querying to determine
  * where Druid segments live in deep storage and then reading those segments into memory in order to avoid straining
  * the Druid cluster. In general, users should not directly instantiate instances of this class but instead use
  * sparkSession.read.format("druid").options(Map(...)).load(). If the schema of the data in Druid is known, overhead
  * can be further reduced by providing it directly (e.g. sparkSession.read.format("druid").schema(schema).options...)
  *
  * To aid comprehensibility, some idiomatic Scala has been somewhat java-fied.
  */
class DruidDataSourceReader(
                             var schema: Option[StructType] = None,
                             conf: Configuration
                           ) extends DataSourceReader
  with SupportsPushDownRequiredColumns with SupportsPushDownFilters with SupportsScanColumnarBatch with Logging {
  private lazy val metadataClient =
    DruidDataSourceReader.createDruidMetaDataClient(conf)
  private lazy val druidClient = DruidDataSourceReader.createDruidClient(conf)

  private var filters: Array[Filter] = Array.empty
  private var druidColumnTypes: Option[Set[String]] = Option.empty

  override def readSchema(): StructType = {
    if (schema.isDefined) {
      schema.get
    } else {
      require(conf.isPresent(DruidConfigurationKeys.tableKey),
        s"Must set ${DruidConfigurationKeys.tableKey}!")
      // TODO: Optionally accept a granularity so that if lowerBound to upperBound spans more than
      //  twice the granularity duration, we can send a list with two disjoint intervals and
      //  minimize the load on the broker from having to merge large numbers of segments
      val (lowerBound, upperBound) = FilterUtils.getTimeFilterBounds(filters)
      val columnMap = druidClient.getSchema(
        conf.getString(DruidConfigurationKeys.tableKey),
        Some(List[Interval](Intervals.utc(
          lowerBound.getOrElse(JodaUtils.MIN_INSTANT),
          upperBound.getOrElse(JodaUtils.MAX_INSTANT)
        )))
      )
      schema = Option(SchemaUtils.convertDruidSchemaToSparkSchema(columnMap))
      druidColumnTypes = Option(columnMap.map(_._2._1).toSet)
      schema.get
    }
  }

  override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
    // For now, one partition for each Druid segment partition
    // Future improvements can use information from SegmentAnalyzer results to do smart things
    if (schema.isEmpty) {
      readSchema()
    }
    val readerConf = conf.dive(DruidConfigurationKeys.readerPrefix)
    val filter = FilterUtils.mapFilters(filters, schema.get)
    val useSparkConfForDeepStorage = readerConf.getBoolean(DruidConfigurationKeys.useSparkConfForDeepStorageDefaultKey)
    val useCompactSketches = readerConf.isPresent(DruidConfigurationKeys.useCompactSketchesKey)
    val useDefaultNullHandling = readerConf.getBoolean(DruidConfigurationKeys.useDefaultValueForNullDefaultKey)

    // Allow passing hard-coded list of segments to load
    if (readerConf.isPresent(DruidConfigurationKeys.segmentsKey)) {
      val segments: JList[DataSegment] = MAPPER.readValue(
        readerConf.getString(DruidConfigurationKeys.segmentsKey),
        new TypeReference[JList[DataSegment]]() {}
      )
      segments.asScala
        .map(segment =>
          new DruidInputPartition(
            segment,
            schema.get,
            filter,
            druidColumnTypes,
            conf,
            useSparkConfForDeepStorage,
            useCompactSketches,
            useDefaultNullHandling
          ): InputPartition[InternalRow]
        ).asJava
    } else {
      getSegments
        .map(segment =>
          new DruidInputPartition(
            segment,
            schema.get,
            filter,
            druidColumnTypes,
            conf,
            useSparkConfForDeepStorage,
            useCompactSketches,
            useDefaultNullHandling
          ): InputPartition[InternalRow]
        ).asJava
    }
  }

  override def pruneColumns(structType: StructType): Unit = {
    schema = Option(structType)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    readSchema()
    val useSqlCompatibleNullHandling = !conf.getBoolean(DruidConfigurationKeys.useDefaultValueForNullDefaultKey)
    filters.partition(FilterUtils.isSupportedFilter(_, schema.get, useSqlCompatibleNullHandling)) match {
      case (supported, unsupported) =>
        this.filters = supported
        unsupported
    }
  }

  override def pushedFilters(): Array[Filter] = filters

  private[v2] def getSegments: Seq[DataSegment] = {
    require(conf.isPresent(DruidConfigurationKeys.tableKey),
      s"Must set ${DruidConfigurationKeys.tableKey}!")

    // Check filters for any bounds on __time
    // Otherwise, we'd need to full scan the segments table
    val (lowerTimeBound, upperTimeBound) = FilterUtils.getTimeFilterBounds(filters)

    metadataClient.getSegmentPayloads(
      conf.getString(DruidConfigurationKeys.tableKey),
      lowerTimeBound,
      upperTimeBound,
      conf.getBoolean(DruidConfigurationKeys.allowIncompletePartitionsDefaultKey)
    )
  }

  override def planBatchInputPartitions(): JList[InputPartition[ColumnarBatch]] = {
    if (schema.isEmpty) {
      readSchema()
    }
    val readerConf = conf.dive(DruidConfigurationKeys.readerPrefix)
    val filter = FilterUtils.mapFilters(filters, schema.get)
    val useSparkConfForDeepStorage = readerConf.getBoolean(DruidConfigurationKeys.useSparkConfForDeepStorageDefaultKey)
    val useCompactSketches = readerConf.isPresent(DruidConfigurationKeys.useCompactSketchesKey)
    val useDefaultNullHandling = readerConf.getBoolean(DruidConfigurationKeys.useDefaultValueForNullDefaultKey)
    val batchSize = readerConf.getInt(DruidConfigurationKeys.batchSizeDefaultKey)

    // Allow passing hard-coded list of segments to load
    if (readerConf.isPresent(DruidConfigurationKeys.segmentsKey)) {
      val segments: JList[DataSegment] = MAPPER.readValue(
        readerConf.getString(DruidConfigurationKeys.segmentsKey),
        new TypeReference[JList[DataSegment]]() {}
      )
      segments.asScala
        .map(segment =>
          new DruidColumnarInputPartition(
            segment,
            schema.get,
            filter,
            druidColumnTypes,
            conf,
            useSparkConfForDeepStorage,
            useCompactSketches,
            useDefaultNullHandling,
            batchSize
          ): InputPartition[ColumnarBatch]
        ).asJava
    } else {
      getSegments
        .map(segment =>
          new DruidColumnarInputPartition(
            segment,
            schema.get,
            filter,
            druidColumnTypes,
            conf,
            useSparkConfForDeepStorage,
            useCompactSketches,
            useDefaultNullHandling,
            batchSize
          ): InputPartition[ColumnarBatch]
        ).asJava
    }
  }

  override def enableBatchRead(): Boolean = {
    // Fail fast
    if (!conf.dive(DruidConfigurationKeys.readerPrefix).getBoolean(DruidConfigurationKeys.vectorizeDefaultKey)) {
      false
    } else {
      if (schema.isEmpty) {
        readSchema()
      }
      val filterOpt = FilterUtils.mapFilters(filters, schema.get)
      filterOpt.fold(true) { filter =>
        val rowSignature = SchemaUtils.generateRowSignatureFromSparkSchema(schema.get)
        val canVectorize = filter.toOptimizedFilter.canVectorizeMatcher(rowSignature)
        if (!canVectorize) {
          logWarn("Vectorization enabled in config but pushed-down filters are not vectorizable! Reading rows.")
        }
        canVectorize
      }
    }
  }
}

object DruidDataSourceReader {
  def apply(schema: StructType, dataSourceOptions: DataSourceOptions): DruidDataSourceReader = {
    new DruidDataSourceReader(Option(schema), Configuration(dataSourceOptions))
  }

  def apply(dataSourceOptions: DataSourceOptions): DruidDataSourceReader = {
    new DruidDataSourceReader(None, Configuration(dataSourceOptions))
  }

  /* Unfortunately, there's no single method of interacting with a Druid cluster that provides all
   * three operations we need: get segment locations, get dataSource schemata, and publish segments.
   *
   * Segment locations can be determined either via direct interaction with the metadata server or
   * via the coordinator API, but not via querying the `sys.segments` table served from a cluster
   * since the `sys.segments` table prunes load specs.
   *
   * Data source schemata can be determined via querying the `INFORMATION_SCHEMA.COLUMNS` table, via
   * SegmentMetadataQueries, or via pulling segments into memory and analyzing them.
   * SegmentMetadataQueries can be expensive and time-consuming for large numbers of segments. This
   * could be worked around by only checking the first and last segments for an interval, which
   * would catch schema evolution that spans the interval to query, but not schema evolution within
   * the interval and would prevent determining accurate statistics. Likewise, pulling segments into
   * memory on the driver to check their schema is expensive and inefficient and has the same schema
   * evolution and accurate statistics problem.
   * The `INFORMATION_SCHEMA.COLUMNS` table does not contain information about whether or not a column
   * could contain multiple values and does not know the actual metric type for complex metrics. Less
   * relevantly, we wouldn't have access to possibly useful statistics about the segments
   * that could be used to perform more efficient reading, and the Druid cluster to read from would
   * need to have sql querying initialized and be running a version of Druid >= 0.14. Since we're
   * not currently doing any intelligent partitioning for reads, this doesn't really matter.
   *
   * Publishing segments can only be done via direct interaction with the metadata server.
   *
   * Since there's no way to satisfy these constraints with a single method of interaction, we will
   * need to use a metadata client and a druid client. The metadata client can fetch segment
   * locations and publish segments, and the druid client will issue SegmentMetadata queries to determine
   * datasource schemata.
   */

  def createDruidMetaDataClient(conf: Configuration): DruidMetadataClient = {
    DruidMetadataClient(conf)
  }

  def createDruidClient(conf: Configuration): DruidClient = {
    DruidClient(conf)
  }
}
