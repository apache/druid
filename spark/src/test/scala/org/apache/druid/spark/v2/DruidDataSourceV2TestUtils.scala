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

package org.apache.druid.spark.v2

import com.google.common.base.{Supplier, Suppliers}
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.druid.java.util.common.{FileUtils, Intervals, StringUtils}
import org.apache.druid.java.util.common.granularity.GranularityType
import org.apache.druid.metadata.{MetadataStorageConnectorConfig, MetadataStorageTablesConfig,
  SQLMetadataConnector}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.DruidConfigurationKeys
import org.apache.druid.spark.registries.SQLConnectorRegistry
import org.apache.druid.spark.utils.SchemaUtils
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.NumberedShardSpec
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, FloatType, LongType,
  StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.joda.time.Interval
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException
import org.skife.jdbi.v2.{DBI, Handle}

import java.io.File
import java.util.{Properties, UUID, List => JList, Map => JMap}
import scala.collection.JavaConverters.{asScalaIteratorConverter,
  collectionAsScalaIterableConverter, mapAsJavaMapConverter, seqAsJavaListConverter}
import scala.collection.mutable.ArrayBuffer

trait DruidDataSourceV2TestUtils {
  val dataSource: String = "spark_druid_test"
  val interval: Interval = Intervals.of("2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z")
  val secondInterval: Interval = Intervals.of("2020-01-02T00:00:00.000Z/2020-01-03T00:00:00.000Z")
  val version: String = "0"
  val segmentsDir: File =
    new File(makePath("src", "test", "resources", "segments")).getCanonicalFile
  val firstSegmentPath: String =
    makePath("spark_druid_test", "2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z", "0", "0", "index.zip")
  val secondSegmentPath: String =
    makePath("spark_druid_test", "2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z", "0", "1", "index.zip")
  val thirdSegmentPath: String =
    makePath("spark_druid_test", "2020-01-02T00:00:00.000Z_2020-01-03T00:00:00.000Z", "0", "0", "index.zip")
  val loadSpec: String => JMap[String, AnyRef] = (path: String) =>
    Map[String, AnyRef]("type" -> "local", "path" -> path).asJava
  val dimensions: JList[String] = List("dim1", "dim2", "id1", "id2").asJava
  val metrics: JList[String] = List(
    "count", "sum_metric1","sum_metric2","sum_metric3","sum_metric4","uniq_id1").asJava
  val metricsSpec: String =
    """[
      |  { "type": "count", "name": "count" },
      |  { "type": "longSum", "name": "sum_metric1", "fieldName": "sum_metric1" },
      |  { "type": "longSum", "name": "sum_metric2", "fieldName": "sum_metric2" },
      |  { "type": "doubleSum", "name": "sum_metric3", "fieldName": "sum_metric3" },
      |  { "type": "floatSum", "name": "sum_metric4", "fieldName": "sum_metric4" },
      |  { "type": "thetaSketch", "name": "uniq_id1", "fieldName": "uniq_id1", "isInputThetaSketch": true }
      |]""".stripMargin
  val binaryVersion: Integer = 9
  val timestampColumn: String = "__time"
  val timestampFormat: String = "auto"
  val segmentGranularity: String = GranularityType.DAY.name

  val firstSegment: DataSegment = new DataSegment(
    dataSource,
    interval,
    version,
    loadSpec(makePath(segmentsDir.getCanonicalPath, firstSegmentPath)),
    dimensions,
    metrics,
    new NumberedShardSpec(0, 0),
    binaryVersion,
    3278L
  )
  val secondSegment: DataSegment = new DataSegment(
    dataSource,
    interval,
    version,
    loadSpec(makePath(segmentsDir.getCanonicalPath, secondSegmentPath)),
    dimensions,
    metrics,
    new NumberedShardSpec(1, 0),
    binaryVersion,
    3299L
  )
  val thirdSegment: DataSegment = new DataSegment(
    dataSource,
    secondInterval,
    version,
    loadSpec(makePath(segmentsDir.getCanonicalPath, thirdSegmentPath)),
    dimensions,
    metrics,
    new NumberedShardSpec(0, 0),
    binaryVersion,
    3409L
  )

  val firstSegmentString: String = MAPPER.writeValueAsString(firstSegment)
  val secondSegmentString: String = MAPPER.writeValueAsString(secondSegment)
  val thirdSegmentString: String = MAPPER.writeValueAsString(thirdSegment)

  val idOneSketch: Array[Byte] = StringUtils.decodeBase64String("AQMDAAA6zJNV0wc7TCHDCQ==")
  val idTwoSketch: Array[Byte] = StringUtils.decodeBase64String("AQMDAAA6zJNHlmybd5/laQ==")
  val idThreeSketch: Array[Byte] = StringUtils.decodeBase64String("AQMDAAA6zJOppPrHQT61Dw==")

  val firstTimeBucket: Long = 1577836800000L
  val secondTimeBucket: Long = 1577923200000L

  val schema: StructType = StructType(Seq[StructField](
    StructField("__time", LongType),
    StructField("dim1", ArrayType(StringType, false)),
    StructField("dim2", StringType),
    StructField("id1", StringType),
    StructField("id2", StringType),
    StructField("count", LongType),
    StructField("sum_metric1", LongType),
    StructField("sum_metric2", LongType),
    StructField("sum_metric3", DoubleType),
    StructField("sum_metric4", FloatType),
    StructField("uniq_id1", BinaryType)
  ))

  val columnTypes: Option[Set[String]] =
    Option(Set("LONG", "STRING", "FLOAT", "DOUBLE", "thetaSketch"))

  private val tempDirs: ArrayBuffer[String] = new ArrayBuffer[String]()
  def testWorkingStorageDirectory: String = {
    val tempDir = FileUtils.createTempDir("druid-spark-tests").getCanonicalPath
    tempDirs += tempDir
    tempDir
  }

  private val testDbUri = "jdbc:derby:memory:TestDatabase"
  def generateUniqueTestUri(): String = testDbUri + dbSafeUUID

  val metadataClientProps: String => Map[String, String] = (uri: String) => Map[String, String](
    s"${DruidConfigurationKeys.metadataPrefix}.${DruidConfigurationKeys.metadataDbTypeKey}" -> "embedded_derby",
    s"${DruidConfigurationKeys.metadataPrefix}.${DruidConfigurationKeys.metadataConnectUriKey}" -> uri
  )

  lazy val writerProps: Map[String, String] = Map[String, String](
    DataSourceOptions.TABLE_KEY -> dataSource,
    s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.versionKey}" -> version,
    s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.storageDirectoryKey}" ->
      testWorkingStorageDirectory,
    s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.dimensionsKey}" ->
      dimensions.asScala.mkString(","),
    s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.metricsKey}" -> metricsSpec,
    s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.timestampColumnKey}" ->
      timestampColumn,
    s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.segmentGranularityKey}" ->
      segmentGranularity
  )

  def createTestDb(uri: String): Unit = new DBI(s"$uri;create=true").open().close()
  def openDbiToTestDb(uri: String): Handle = new DBI(uri).open()
  def tearDownTestDb(uri: String): Unit = {
    try {
      new DBI(s"$uri;shutdown=true").open().close()
    } catch {
      // Closing an in-memory Derby database throws an expected exception. It bubbles up as an
      // UnableToObtainConnectionException from skiffie.
      // TODO: Just open the connection directly and check the exception there
      case _: UnableToObtainConnectionException =>
    }}

  def registerEmbeddedDerbySQLConnector(): Unit = {
    SQLConnectorRegistry.register("embedded_derby",
      (connectorConfigSupplier: Supplier[MetadataStorageConnectorConfig],
       metadataTableConfigSupplier: Supplier[MetadataStorageTablesConfig]) => {
        val connectorConfig = connectorConfigSupplier.get()
        val amendedConnectorConfigSupplier =
          new MetadataStorageConnectorConfig
          {
            override def isCreateTables: Boolean = true
            override def getHost: String = connectorConfig.getHost
            override def getPort: Int = connectorConfig.getPort
            override def getConnectURI: String = connectorConfig.getConnectURI
            override def getUser: String = connectorConfig.getUser
            override def getPassword: String = connectorConfig.getPassword
            override def getDbcpProperties: Properties = connectorConfig.getDbcpProperties
          }

        val res: SQLMetadataConnector =
          new SQLMetadataConnector(Suppliers.ofInstance(amendedConnectorConfigSupplier), metadataTableConfigSupplier) {
            val datasource: BasicDataSource = getDatasource
            datasource.setDriverClassLoader(getClass.getClassLoader)
            datasource.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            private val dbi = new DBI(connectorConfigSupplier.get().getConnectURI)
            private val SERIAL_TYPE = "BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"

            override def getSerialType: String = SERIAL_TYPE

            override def getStreamingFetchSize: Int = 1

            override def getQuoteString: String = "\\\""

            override def tableExists(handle: Handle, tableName: String): Boolean =
              !handle.createQuery("select * from SYS.SYSTABLES where tablename = :tableName")
                .bind("tableName", StringUtils.toUpperCase(tableName)).list.isEmpty;

            override def getDBI: DBI = dbi
          }
        res.createSegmentTable()
        res
      })
  }

  def cleanUpWorkingDirectory(): Unit = {
    tempDirs.foreach(dir => FileUtils.deleteDirectory(new File(dir).getCanonicalFile))
  }

  def partitionReaderToSeq(reader: InputPartitionReader[InternalRow]): Seq[InternalRow] = {
    val res = new ArrayBuffer[InternalRow]()
    while (reader.next()) {
      res += reader.get()
    }
    reader.close()
    res
  }

  def columnarPartitionReaderToSeq(reader: InputPartitionReader[ColumnarBatch]): Seq[InternalRow] = {
    val res = new ArrayBuffer[InternalRow]()
    // ColumnarBatches return MutableColumnarRows, so we need to copy them before we close
    while (reader.next()) {
      val batch = reader.get()
      batch.rowIterator().asScala.foreach { row =>
        // MutableColumnarRows don't support copying ArrayTypes, we can't use row.copy()
        val finalizedRow = new GenericInternalRow(batch.numCols())
        (0 until batch.numCols()).foreach{ col =>
          if (row.isNullAt(col)) {
            finalizedRow.setNullAt(col)
          } else {
            val dataType = batch.column(col).dataType()
            dataType match {
              case _: ArrayType =>
                // Druid only supports multiple values for Strings, hard-code that assumption here for now
                val finalizedArr = row.getArray(col).array.map(el => el.asInstanceOf[UTF8String].copy())
                finalizedRow.update(col, ArrayData.toArrayData(finalizedArr))
              case _ =>
                finalizedRow.update(col, row.get(col, dataType))
            }
          }
        }
        res += finalizedRow
      }
    }
    reader.close()
    res
  }

  def wrapSeqToInternalRow(seq: Seq[Any], schema: StructType): InternalRow = {
    InternalRow.fromSeq(seq.zipWithIndex.map{case (elem, i) =>
      if (elem == null) { // scalastyle:ignore null
        null // scalastyle:ignore null
      } else {
        schema(i).dataType match {
          case _: ArrayType =>
            val baseType = schema(i).dataType.asInstanceOf[ArrayType].elementType
            elem match {
              case collection: Traversable[_] =>
                ArrayData.toArrayData(collection.map { elem =>
                  SchemaUtils.parseToScala(elem, baseType)
                })
              case _ =>
                // Single-element arrays
                ArrayData.toArrayData(List(SchemaUtils.parseToScala(elem, baseType)))
            }
          case _ => SchemaUtils.parseToScala(elem, schema(i).dataType)
        }
      }
    })
  }

  /**
    * Given a DataFrame DF, return a collection of arrays of Rows where each array contains all rows for a
    * partition in DF.
    *
    * @param df The dataframe to extract partitions from.
    * @return A Seq[Array[Row]], where each Array[Row] contains all rows for a corresponding partition in DF.
    */
  def getDataFramePartitions(df: DataFrame): Seq[Array[Row]] = {
    df
      .rdd
      .map(row => TaskContext.getPartitionId() -> row)
      .collect()
      .groupBy(_._1)
      .values
      .map(_.map(_._2))
      .toSeq
  }

  def makePath(components: String*): String = {
    components.mkString(File.separator)
  }

  def dbSafeUUID: String = StringUtils.removeChar(UUID.randomUUID.toString, '-')
}
