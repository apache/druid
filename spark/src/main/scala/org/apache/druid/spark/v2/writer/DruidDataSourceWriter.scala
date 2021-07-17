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

package org.apache.druid.spark.v2.writer

import org.apache.druid.java.util.common.ISE
import org.apache.druid.segment.loading.DataSegmentKiller
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.clients.DruidMetadataClient
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.SegmentWriterRegistry
import org.apache.druid.spark.utils.SegmentRationalizer
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.util.Optional
import scala.collection.JavaConverters.{mapAsScalaMapConverter, seqAsJavaListConverter}

/**
  * A DruidDataSourceWriter orchestrates writing a dataframe to Druid.
  *
  * @param schema The schema of the dataframe to be written to Druid.
  * @param conf A set of options to configure the DruidDataWriterFactories, DruidDataWriters, and
  *             clients used to write a dataframe to Druid
  * @param metadataClient The client to use to read from and write to a Druid metadata server.
  */
class DruidDataSourceWriter(
                             schema: StructType,
                             conf: Configuration,
                             metadataClient: DruidMetadataClient
                           ) extends DataSourceWriter with Logging {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new DruidDataWriterFactory(schema, conf)
  }

  /**
    * Commit the segment locations listed in WRITERCOMMITMESSAGES to a Druid metadata server.
    *
    * This is the only place we have a centralized view of all generated segments, so we also take this opportunity
    * to rationalize the shard specs of the generated segments to ensure that generated partitions form a contiguous and
    * complete segment for each interval written. In the future, this could allow respecting a maxRowsPerPartition
    * argument and writing to multiple data sources from a single .write() call. By default this rationalization occurs
    * whenever a partition map is not specified, but it can also be controlled directly via the "rationalizeSegments"
    * configuration.
    *
    * @param writerCommitMessages An array of messages containing sequences of serialized
    *                             DataSegments written to deep storage.
    */
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val segments =
      writerCommitMessages.flatMap(_.asInstanceOf[DruidWriterCommitMessage].serializedSegments)

    val writerConf = conf.dive(DruidConfigurationKeys.writerPrefix)
    writerConf.isPresent(DruidConfigurationKeys.partitionMapKey)
    val isPartitionMapDefined = writerConf.isPresent(DruidConfigurationKeys.partitionMapKey)
    val rationalizedSegments = if (
      writerConf.getBoolean(DruidConfigurationKeys.rationalizeSegmentsKey, !isPartitionMapDefined)
    ) {
      SegmentRationalizer.rationalizeSegments(segments.map(MAPPER.readValue(_, classOf[DataSegment])))
    } else {
      segments.map(MAPPER.readValue(_, classOf[DataSegment])).toSeq
    }

    logInfo(s"Committing the following segments: ${rationalizedSegments.map(_.toString).mkString(", ")}")

    metadataClient.publishSegments(rationalizedSegments.toList.asJava, MAPPER)
  }

  // Clean up segments in deep storage but not in metadata
  /**
    * Clean up a failed write by deleting any segments already written to deep storage.
    *
    * @param writerCommitMessages An array of messages containing sequences of serialized
    *                             DataSegments that have already been written to deep storage.
    */
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val segments =
      writerCommitMessages.flatMap(_.asInstanceOf[DruidWriterCommitMessage].serializedSegments)

    logWarn(s"Aborting the following commits: ${segments.mkString(", ")}")

    val segmentKiller: DataSegmentKiller = SegmentWriterRegistry.getSegmentKiller(
      conf.dive(DruidConfigurationKeys.writerPrefix).get(DruidConfigurationKeys.deepStorageTypeDefaultKey),
      conf
    )
    segments.foreach { segmentStr =>
      val segment = MAPPER.readValue(segmentStr, classOf[DataSegment])
      // Not using segmentKiller.killQuietly so we can surface the exception at WARN instead of DEBUG
      try {
        segmentKiller.kill(segment)
      } catch {
        case e: Exception =>
          log.warn(s"Unable to clean segment ${segment.getId.toString} with load spec " +
            s"${segment.getLoadSpec.asScala.mkString(", ")}! Caught ${e.getMessage}", e)
      }
    }
  }
}

object DruidDataSourceWriter extends Logging {
  def apply(
             schema: StructType,
             saveMode: SaveMode,
             conf: Configuration
           ): Optional[DataSourceWriter] = {
    validateDataSourceOption(conf, schema)
    val dataSource = conf.getString(DruidConfigurationKeys.tableKey)
    val metadataClient = DruidMetadataClient(conf)
    val dataSourceExists = metadataClient.checkIfDataSourceExists(dataSource)
    saveMode match {
      case SaveMode.Append =>
        // In theory, if a caller provided an interval in the data source options, we could check
        // to see if the interval already existed or not, and only throw an error if the data source
        // already had data for the given interval. We'd want to also plumb that interval through to
        // DruidDataWriter instances to make sure they dropped rows outside of the interval.
        throw new UnsupportedOperationException(
          "Druid does not support appending to existing dataSources, only reindexing!"
        )
      case SaveMode.ErrorIfExists => if (dataSourceExists) {
        throw new ISE(s"$dataSource already exists!")
      } else {
        createDataSourceWriterOptional(schema, conf, metadataClient)
      }
      case SaveMode.Ignore => if (dataSourceExists) {
        logInfo(s"$dataSource already exists and Save Mode is Ignore; not writing!")
        Optional.empty[DataSourceWriter]
      } else {
        createDataSourceWriterOptional(schema, conf, metadataClient)
      }
      case SaveMode.Overwrite =>
        createDataSourceWriterOptional(schema, conf, metadataClient)
    }
  }

  private[v2] def createDataSourceWriterOptional(
                                                  schema: StructType,
                                                  conf: Configuration,
                                                  metadataClient: DruidMetadataClient
                                                ): Optional[DataSourceWriter] = {
    Optional.of[DataSourceWriter](new DruidDataSourceWriter(
      schema, conf, metadataClient)
    )
  }

  private[v2] def validateDataSourceOption(conf: Configuration, schema: StructType): Unit = {
    require(conf.isPresent(DruidConfigurationKeys.tableKey),
      s"Must set ${DruidConfigurationKeys.tableKey}!")

    require(conf.isPresent(DruidConfigurationKeys.metadataPrefix, DruidConfigurationKeys.metadataDbTypeKey),
      s"Must set ${DruidConfigurationKeys.metadataPrefix}.${DruidConfigurationKeys.metadataDbTypeKey}"
    )
    val tsColumn =
      conf.dive(DruidConfigurationKeys.writerPrefix).get(DruidConfigurationKeys.timeStampColumnDefaultKey)
    require(
      schema.fieldNames.contains(tsColumn),
      s"$tsColumn must be a field in the dataframe to write!"
    )
  }
}
