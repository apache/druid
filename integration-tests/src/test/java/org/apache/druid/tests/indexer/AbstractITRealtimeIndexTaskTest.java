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

package org.apache.druid.tests.indexer;

import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.curator.discovery.ServerDiscoveryFactory;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.RetryUtil;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Steps
 * 1) Submit a realtime index task
 * 2) Load data using EventReceiverFirehose
 * 3) Run queries to verify that the ingested data is available for queries
 * 4) Wait for handover of the segment to historical node
 * 5) Query data (will be from historical node)
 * 6) Disable and delete the created data segment
 */
public abstract class AbstractITRealtimeIndexTaskTest extends AbstractIndexerTest
{
  static final String EVENT_RECEIVER_SERVICE_NAME = "eventReceiverServiceName";
  static final String EVENT_DATA_FILE = "/indexer/wikipedia_realtime_index_data.json";

  private static final Logger LOG = new Logger(AbstractITRealtimeIndexTaskTest.class);
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";

  static final int DELAY_BETWEEN_EVENTS_SECS = 4;
  final String TIME_PLACEHOLDER = "YYYY-MM-DDTHH:MM:SS";
  // format for putting datestamp into events
  static final DateTimeFormatter EVENT_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
  // format for the querying interval
  private static final DateTimeFormatter INTERVAL_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:'00Z'");
  // format for the expected timestamp in a query response
  private static final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
  DateTime dtFirst;            // timestamp of 1st event
  DateTime dtLast;             // timestamp of last event
  DateTime dtGroupBy;          // timestamp for expected response for groupBy query

  @Inject
  ServerDiscoveryFactory factory;
  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  private String fullDatasourceName;

  void doTest()
  {
    fullDatasourceName = INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix();

    LOG.info("Starting test: %s", this.getClass().getSimpleName());
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // the task will run for 3 minutes and then shutdown itself
      String task = setShutOffTime(
          getResourceAsString(getTaskResource()),
          DateTimes.utc(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(3))
      );
      task = StringUtils.replace(task, "%%DATASOURCE%%", fullDatasourceName);

      LOG.info("indexerSpec: [%s]\n", task);
      String taskID = indexer.submitTask(task);


      // sleep for a while to let peons finish starting up
      TimeUnit.SECONDS.sleep(5);

      // this posts 22 events, one every 4 seconds
      // each event contains the current time as its timestamp except
      //   the timestamp for the 14th event is early enough that the event should be ignored
      //   the timestamp for the 18th event is 2 seconds earlier than the 17th
      postEvents();

      // sleep for a while to let the events be ingested
      TimeUnit.SECONDS.sleep(5);

      // put the timestamps into the query structure
      String query_response_template;
      InputStream is = ITRealtimeIndexTaskTest.class.getResourceAsStream(getQueriesResource());
      if (null == is) {
        throw new ISE("could not open query file: %s", getQueriesResource());
      }
      query_response_template = IOUtils.toString(is, StandardCharsets.UTF_8);

      String queryStr = query_response_template;
      queryStr = StringUtils.replace(queryStr, "%%TIMEBOUNDARY_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst));
      queryStr = StringUtils.replace(queryStr, "%%TIMEBOUNDARY_RESPONSE_MAXTIME%%", TIMESTAMP_FMT.print(dtLast));
      queryStr = StringUtils.replace(queryStr, "%%TIMEBOUNDARY_RESPONSE_MINTIME%%", TIMESTAMP_FMT.print(dtFirst));
      queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_QUERY_START%%", INTERVAL_FMT.print(dtFirst));
      queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_QUERY_END%%", INTERVAL_FMT.print(dtLast.plusMinutes(2)));
      queryStr = StringUtils.replace(queryStr, "%%TIMESERIES_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst));
      queryStr = StringUtils.replace(queryStr, "%%POST_AG_REQUEST_START%%", INTERVAL_FMT.print(dtFirst));
      queryStr = StringUtils.replace(queryStr, "%%POST_AG_REQUEST_END%%", INTERVAL_FMT.print(dtLast.plusMinutes(2)));
      String postAgResponseTimestamp = TIMESTAMP_FMT.print(dtGroupBy.withSecondOfMinute(0));
      queryStr = StringUtils.replace(queryStr, "%%POST_AG_RESPONSE_TIMESTAMP%%", postAgResponseTimestamp);
      queryStr = StringUtils.replace(queryStr, "%%DATASOURCE%%", fullDatasourceName);

      // should hit the queries all on realtime task or some on realtime task
      // and some on historical.  Which it is depends on where in the minute we were
      // when we started posting events.
      try {
        this.queryHelper.testQueriesFromString(getRouterURL(), queryStr, 2);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      // wait for the task to complete
      indexer.waitUntilTaskCompletes(taskID);

      // task should complete only after the segments are loaded by historical node
      RetryUtil.retryUntil(
          () -> coordinator.areSegmentsLoaded(fullDatasourceName),
          true,
          10000,
          60,
          "Real-time generated segments loaded"
      );

      // queries should be answered by historical
      this.queryHelper.testQueriesFromString(getRouterURL(), queryStr, 2);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String setShutOffTime(String taskAsString, DateTime time)
  {
    return StringUtils.replace(taskAsString, "#SHUTOFFTIME", time.toString());
  }

  private String getRouterURL()
  {
    return StringUtils.format(
        "%s/druid/v2?pretty",
        config.getRouterUrl()
    );
  }

  abstract String getTaskResource();
  abstract String getQueriesResource();

  abstract void postEvents() throws Exception;
}
