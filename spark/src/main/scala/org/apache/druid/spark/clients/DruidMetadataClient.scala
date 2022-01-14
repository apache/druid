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

package org.apache.druid.spark.clients

import com.fasterxml.jackson.core.`type`.TypeReference
import com.google.common.base.Suppliers
import org.apache.druid.indexer.SQLMetadataStorageUpdaterJobHandler
import org.apache.druid.java.util.common.{DateTimes, Intervals, JodaUtils, StringUtils}
import org.apache.druid.metadata.{DynamicConfigProvider, MetadataStorageConnectorConfig,
  MetadataStorageTablesConfig, SQLMetadataConnector}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.SQLConnectorRegistry
import org.apache.druid.timeline.{DataSegment, Partitions, VersionedIntervalTimeline}
import org.skife.jdbi.v2.{DBI, Handle}

import java.util.Properties
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter,
  asScalaSetConverter, mapAsJavaMapConverter}

class DruidMetadataClient(
                           metadataDbType: String,
                           host: String,
                           port: Int,
                           connectUri: String,
                           user: String,
                           passwordProviderSer: String,
                           dbcpMap: Properties,
                           base: String = "druid"
                         ) extends Logging {
  private lazy val druidMetadataTableConfig = MetadataStorageTablesConfig.fromBase(base)
  private lazy val dbcpProperties = new Properties(dbcpMap)
  private lazy val password = if (passwordProviderSer == "") {
    // Jackson doesn't like deserializing empty strings
    passwordProviderSer
  } else {
    MAPPER.readValue[DynamicConfigProvider[String]](
      passwordProviderSer, new TypeReference[DynamicConfigProvider[String]] {}
    ).getConfig.getOrDefault("password", "")
  }

  private lazy val connectorConfig: MetadataStorageConnectorConfig =
    new MetadataStorageConnectorConfig
    {
      override def isCreateTables: Boolean = false
      override def getHost: String = host
      override def getPort: Int = port
      override def getConnectURI: String = connectUri
      override def getUser: String = user
      override def getPassword: String = password
      override def getDbcpProperties: Properties = dbcpProperties
    }
  private lazy val connectorConfigSupplier = Suppliers.ofInstance(connectorConfig)
  private lazy val metadataTableConfigSupplier = Suppliers.ofInstance(druidMetadataTableConfig)
  private lazy val connector = buildSQLConnector()

  /**
    * Get the non-overshadowed used segments for DATASOURCE between INTERVALSTART and INTERVALEND. If either interval
    * endpoint is None, JodaUtils.MIN_INSTANCE/MAX_INSTANCE is used instead. By default, only segments for complete
    * partitions are returned. This behavior can be overriden by setting ALLOWINCOMPLETEPARTITIONS, in which case all
    * non-overshadowed segments in the interval will be returned, regardless of completesness.
    *
    * @param datasource The Druid data source to get segment payloads for.
    * @param intervalStart The start of the interval to fetch segment payloads for. If None, MIN_INSTANT is used.
    * @param intervalEnd The end of the interval to fetch segment payloads for. If None, MAX_INSTANT is used.
    * @param allowIncompletePartitions Whether or not to include segments from incomplete partitions
    * @return A Sequence of DataSegments representing all non-overshadowed used segments for the given data source and
    *         interval.
    */
  def getSegmentPayloads(
                          datasource: String,
                          intervalStart: Option[Long],
                          intervalEnd: Option[Long],
                          allowIncompletePartitions: Boolean = false
                        ): Seq[DataSegment] = {
    val dbi: DBI = connector.getDBI
    val interval = Intervals.utc(
      intervalStart.getOrElse(JodaUtils.MIN_INSTANT), intervalEnd.getOrElse(JodaUtils.MAX_INSTANT)
    )
    logInfo(s"Fetching segment payloads for interval [${interval.toString}] on datasource [$datasource].")
    val startClause = if (intervalStart.isDefined) " AND start >= :start" else ""
    val endClause = if (intervalEnd.isDefined) " AND \"end\" <= :end" else ""
    val allSegments = dbi.withHandle((handle: Handle) => {
      val statement =
        s"""
           |SELECT payload FROM ${druidMetadataTableConfig.getSegmentsTable}
           |WHERE datasource = :datasource AND used = true$startClause$endClause
        """.stripMargin

      val bindMap = Seq(Some("datasource" -> datasource),
        intervalStart.map(bound => "start" -> DateTimes.utc(bound).toString("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")),
        intervalEnd.map(bound => "end" -> DateTimes.utc(bound).toString("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      ).flatten.toMap

      val query = handle.createQuery(statement)
      val result = query
        .bindFromMap(bindMap.asJava)
        .mapTo(classOf[Array[Byte]]).list().asScala
      result.map(blob =>
        MAPPER.readValue[DataSegment](
          StringUtils.fromUtf8(blob), new TypeReference[DataSegment] {}
        )
      )
    })
    val activeSegments = VersionedIntervalTimeline.forSegments(allSegments.asJava).findNonOvershadowedObjectsInInterval(
      interval,
      if (allowIncompletePartitions) Partitions.INCOMPLETE_OK else Partitions.ONLY_COMPLETE
    ).asScala.toSeq
    logInfo(s"Fetched payloads for ${activeSegments.size} segments for interval [${interval.toString}] on " +
      s"datasource [$datasource].")
    activeSegments
  }

  def publishSegments(segments: java.util.List[DataSegment]): Unit = {
    val metadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(connector)
    metadataStorageUpdaterJobHandler.publishSegments(druidMetadataTableConfig.getSegmentsTable,
      segments, MAPPER)
    logInfo(s"Published ${segments.size()} segments.")
  }

  def checkIfDataSourceExists(dataSource: String): Boolean = {
    val dbi: DBI = connector.getDBI
    dbi.withHandle((handle: Handle) => {
      val statement =
        s"""
           |SELECT DISTINCT dataSource FROM ${druidMetadataTableConfig.getSegmentsTable}
           |WHERE used = true AND dataSource = :dataSource
         """.stripMargin
      !handle.createQuery(statement).bind("dataSource", dataSource).list().isEmpty
    })
  }

  /**
    * This won't run in a Druid cluster, so users will need to respecify metadata connection info.
    * This also means users will need to specifically include the extension jars on their clusters.
    *
    * @return
    */
  private def buildSQLConnector(): SQLMetadataConnector = {
    SQLConnectorRegistry.create(metadataDbType, connectorConfigSupplier, metadataTableConfigSupplier)
  }
}

object DruidMetadataClient {
  def apply(conf: Configuration): DruidMetadataClient = {
    val metadataConf = conf.dive(DruidConfigurationKeys.metadataPrefix)

    require(metadataConf.isPresent(DruidConfigurationKeys.metadataDbTypeKey),
      s"Must set ${DruidConfigurationKeys.metadataPrefix}." +
        s"${DruidConfigurationKeys.metadataDbTypeKey} or provide segments directly!")
    val dbcpProperties = if (metadataConf.isPresent(DruidConfigurationKeys.metadataDbcpPropertiesKey)) {
      MAPPER.readValue[Properties](metadataConf.getString(DruidConfigurationKeys.metadataDbcpPropertiesKey),
        new TypeReference[Properties] {})
    } else {
      new Properties()
    }

    new DruidMetadataClient(
      metadataConf.getAs[String](DruidConfigurationKeys.metadataDbTypeKey),
      metadataConf.get(DruidConfigurationKeys.metadataHostDefaultKey),
      metadataConf.getInt(DruidConfigurationKeys.metadataPortDefaultKey),
      metadataConf.getString(DruidConfigurationKeys.metadataConnectUriKey),
      metadataConf.getString(DruidConfigurationKeys.metadataUserKey),
      metadataConf.getString(DruidConfigurationKeys.metadataPasswordKey),
      dbcpProperties,
      metadataConf.get(DruidConfigurationKeys.metadataBaseNameDefaultKey)
    )
  }
}
