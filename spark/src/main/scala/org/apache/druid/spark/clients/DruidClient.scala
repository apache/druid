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
import com.google.common.net.HostAndPort
import org.apache.druid.java.util.common.{IAE, ISE, Intervals, JodaUtils, StringUtils}
import org.apache.druid.java.util.http.client.response.{StringFullResponseHandler,
  StringFullResponseHolder}
import org.apache.druid.java.util.http.client.{HttpClient, Request}
import org.apache.druid.query.Druids
import org.apache.druid.query.metadata.metadata.{ColumnAnalysis, SegmentAnalysis,
  SegmentMetadataQuery}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.mixins.Logging
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpResponseStatus}
import org.joda.time.{Duration, Interval}

import java.net.URL
import java.util.{List => JList}
import javax.ws.rs.core.MediaType
import scala.annotation.tailrec
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter,
  mapAsScalaMapConverter, seqAsJavaListConverter}

/**
  * Going with SegmentMetadataQueries despite the significant overhead because there's no other way
  * to get accurate information about Druid columns.
  *
  * This is pulled pretty directly from the druid-hadoop-input-format project on GitHub. There is
  * likely substantial room for improvement.
  */
class DruidClient(
                   httpClient: HttpClient,
                   hostAndPort: HostAndPort,
                   numRetries: Int,
                   retryWaitSeconds: Int,
                   timeoutWaitMilliseconds: Int
                 ) extends Logging {
  private val druidBaseQueryURL: HostAndPort => String =
    (hostAndPort: HostAndPort) => s"http://$hostAndPort/druid/v2/"

  private val DefaultSegmentMetadataInterval = List[Interval](Intervals.utc(
    JodaUtils.MIN_INSTANT,
    JodaUtils.MAX_INSTANT
  ))

  /**
    * The SQL system catalog tables are incorrect for multivalue columns and don't have accurate
    * type info, so we need to fall back to segmentMetadataQueries. Given a DATASOURCE and a range
    * of INTERVALS to query over, return a map from column name to a tuple of
    * (columnType, hasMultipleValues). Note that this is a very expensive operation over large
    * numbers of segments. If possible, this method should only be called with the most granular
    * starting and ending intervals instead of over a larger interval.
    *
    * @param dataSource The Druid dataSource to fetch the schema for.
    * @param intervals  The intervals to return the schema for, or None to query all segments.
    * @return A map from column name to data type and whether or not the column is multi-value
    *         for the schema of DATASOURCE.
    */
  def getSchema(dataSource: String, intervals: Option[List[Interval]]): Map[String, (String, Boolean)] = {
    val queryInterval = intervals.getOrElse(DefaultSegmentMetadataInterval)
    val body = Druids.newSegmentMetadataQueryBuilder()
      .dataSource(dataSource)
      .intervals(queryInterval.asJava)
      .analysisTypes(SegmentMetadataQuery.AnalysisType.SIZE)
      .merge(true)
      .context(Map[String, AnyRef](
        "timeout" -> Int.box(timeoutWaitMilliseconds)
      ).asJava)
      .build()
    val response = sendRequestWithRetry(
      druidBaseQueryURL(hostAndPort), numRetries, Option(MAPPER.writeValueAsBytes(body))
    )
    val segments =
      MAPPER.readValue[JList[SegmentAnalysis]](
        response.getContent, new TypeReference[JList[SegmentAnalysis]]() {})
    if (segments.size() == 0) {
      throw new IAE(
        s"No segments found for intervals [${intervals.mkString(",")}] on $dataSource"
      )
    }
    // Since we're setting merge to true, there should only be one item in the list
    if (segments.size() > 1) {
      throw new ISE("Merged segment metadata response had more than one row!")
    }
    log.debug(segments.asScala.map(_.toString).mkString("SegmentAnalysis: [", ", ", "]"))
    /*
     * If a dimension has multiple types within the spanned interval, the resulting column
     * analysis will have the type "STRING" and be an error message. We abuse that here to infer
     * a string type for the dimension and widen the type for the resulting DataFrame.
     */
    val columns = segments.asScala.head.getColumns.asScala.toMap
    columns.foreach{ case(key, column) =>
      if (column.isError) {
        log.warn(s"Multiple column types found for dimension $key in interval" +
          s" ${queryInterval.mkString("[", ", ", "]")}! Falling back to STRING type")
      }
    }
    columns.map{ case (name: String, col: ColumnAnalysis) =>
      name -> (col.getType, col.isHasMultipleValues)
    }
  }

  /*
   * Marking this method as tail recursive because it is for now. If a finally block,
   * special error handling, or more involved set up and tear down code is added, this
   * method may no longer be tail recursive and so compilation will fail. Because the
   * number of retries is user-configurable and will likely be relatively small,
   * latency in communication with a Druid cluster for Segment Metadata will be dominated
   * by the query time, and Scala will optimize tail recursive calls regardless of annotation,
   * future developers shouldn't be concerned if they need to remove this annotation.
   */
  @tailrec
  private def sendRequestWithRetry(
                                    url: String,
                                    retryCount: Int,
                                    content: Option[Array[Byte]] = None
                                  ): StringFullResponseHolder = {
    try {
      sendRequest(url, content)
    } catch {
      case e: Exception =>
        if (retryCount > 0) {
          logInfo(s"Got exception: ${e.getMessage}, retrying ...")
          Thread.sleep(retryWaitSeconds * 1000)
          sendRequestWithRetry(url, retryCount - 1, content)
        } else {
          throw e
        }
    }
  }

  private def sendRequest(url: String, content: Option[Array[Byte]]): StringFullResponseHolder = {
    try {
      val request = buildRequest(url, content)
      var response = httpClient.go(
        request,
        new StringFullResponseHandler(StringUtils.UTF8_CHARSET),
        Duration.millis(timeoutWaitMilliseconds)
      ).get
      if (response.getStatus == HttpResponseStatus.TEMPORARY_REDIRECT) {
        val newUrl = response.getResponse.headers().get("Location")
        logInfo(s"Got a redirect, new location: $newUrl")
        response = httpClient.go(
          buildRequest(newUrl, content), new StringFullResponseHandler(StringUtils.UTF8_CHARSET)
        ).get
      }
      if (!(response.getStatus == HttpResponseStatus.OK)) {
        throw new ISE(
          s"Error getting response for %s, status[%s] content[%s]",
          url,
          response.getStatus,
          response.getContent
        )
      }
      response
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def buildRequest(url: String, content: Option[Array[Byte]]): Request = {
    content.map(
      new Request(HttpMethod.POST, new URL(url))
        .setHeader("Content-Type", MediaType.APPLICATION_JSON)
        .setContent(_)
    ).getOrElse(
      new Request(HttpMethod.GET, new URL(url))
    )
  }
}

object DruidClient {
  // TODO: Add support for Kerberized etc. clients
  def apply(conf: Configuration): DruidClient = {
    val brokerConf = conf.dive(DruidConfigurationKeys.brokerPrefix)
    new DruidClient(
      HttpClientHolder.create.get,
      HostAndPort.fromParts(
        brokerConf.get(DruidConfigurationKeys.brokerHostDefaultKey),
        brokerConf.getInt(DruidConfigurationKeys.brokerPortDefaultKey)),
      brokerConf.getInt(DruidConfigurationKeys.numRetriesDefaultKey),
      brokerConf.getInt(DruidConfigurationKeys.retryWaitSecondsDefaultKey),
      brokerConf.getInt(DruidConfigurationKeys.timeoutMillisecondsDefaultKey)
    )
  }
}

