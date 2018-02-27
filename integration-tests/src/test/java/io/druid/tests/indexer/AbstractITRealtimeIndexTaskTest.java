/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.tests.indexer;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.TestClient;
import io.druid.testing.utils.RetryUtil;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.InputStream;
import java.util.concurrent.Callable;
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
  String taskID;
  final String TIME_PLACEHOLDER = "YYYY-MM-DDTHH:MM:SS";
  // format for putting datestamp into events
  final DateTimeFormatter EVENT_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
  // format for the querying interval
  final DateTimeFormatter INTERVAL_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:'00Z'");
  // format for the expected timestamp in a query response
  final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
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

  void doTest() throws Exception
  {
    LOG.info("Starting test: ITRealtimeIndexTaskTest");
    try {
      // the task will run for 3 minutes and then shutdown itself
      String task = setShutOffTime(
          getTaskAsString(getTaskResource()),
          DateTimes.utc(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(3))
      );
      LOG.info("indexerSpec: [%s]\n", task);
      taskID = indexer.submitTask(task);

      // this posts 22 events, one every 4 seconds
      // each event contains the current time as its timestamp except
      //   the timestamp for the 14th event is early enough that the event should be ignored
      //   the timestamp for the 18th event is 2 seconds earlier than the 17th
      postEvents();

      // sleep for a while to let the events be ingested
      TimeUnit.SECONDS.sleep(5);

      // put the timestamps into the query structure
      String query_response_template = null;
      InputStream is = ITRealtimeIndexTaskTest.class.getResourceAsStream(getQueriesResource());
      if (null == is) {
        throw new ISE("could not open query file: %s", getQueriesResource());
      }
      query_response_template = IOUtils.toString(is, "UTF-8");

      String queryStr = query_response_template
          .replace("%%TIMEBOUNDARY_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst))
          .replace("%%TIMEBOUNDARY_RESPONSE_MAXTIME%%", TIMESTAMP_FMT.print(dtLast))
          .replace("%%TIMEBOUNDARY_RESPONSE_MINTIME%%", TIMESTAMP_FMT.print(dtFirst))
          .replace("%%TIMESERIES_QUERY_START%%", INTERVAL_FMT.print(dtFirst))
          .replace("%%TIMESERIES_QUERY_END%%", INTERVAL_FMT.print(dtLast.plusMinutes(2)))
          .replace("%%TIMESERIES_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtFirst))
          .replace("%%POST_AG_REQUEST_START%%", INTERVAL_FMT.print(dtFirst))
          .replace("%%POST_AG_REQUEST_END%%", INTERVAL_FMT.print(dtLast.plusMinutes(2)))
          .replace("%%POST_AG_RESPONSE_TIMESTAMP%%", TIMESTAMP_FMT.print(dtGroupBy.withSecondOfMinute(0))
          );

      // should hit the queries all on realtime task or some on realtime task
      // and some on historical.  Which it is depends on where in the minute we were
      // when we started posting events.
      try {
        this.queryHelper.testQueriesFromString(getRouterURL(), queryStr, 2);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      // wait for the task to complete
      indexer.waitUntilTaskCompletes(taskID);

      // task should complete only after the segments are loaded by historical node
      RetryUtil.retryUntil(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              return coordinator.areSegmentsLoaded(INDEX_DATASOURCE);
            }
          },
          true,
          60000,
          10,
          "Real-time generated segments loaded"
      );

      // queries should be answered by historical
      this.queryHelper.testQueriesFromString(getRouterURL(), queryStr, 2);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      unloadAndKillData(INDEX_DATASOURCE);
    }
  }

  String setShutOffTime(String taskAsString, DateTime time)
  {
    return taskAsString.replace("#SHUTOFFTIME", time.toString());
  }

  String getRouterURL()
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
