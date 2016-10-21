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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.clients.EventReceiverFirehoseTestClient;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import io.druid.testing.utils.ServerDiscoveryUtil;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
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
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITRealtimeIndexTaskTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITRealtimeIndexTaskTest.class);
  private static final String REALTIME_TASK_RESOURCE = "/indexer/wikipedia_realtime_index_task.json";
  private static final String EVENT_RECEIVER_SERVICE_NAME = "eventReceiverServiceName";
  private static final String EVENT_DATA_FILE = "/indexer/wikipedia_realtime_index_data.json";
  private static final String REALTIME_QUERIES_RESOURCE = "/indexer/wikipedia_realtime_index_queries.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";

  private static final int DELAY_BETWEEN_EVENTS_SECS = 4;
  private String taskID;
  private final String TIME_PLACEHOLDER = "YYYY-MM-DDTHH:MM:SS";
  // format for putting datestamp into events
  private final DateTimeFormatter EVENT_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
  // format for the querying interval
  private final DateTimeFormatter INTERVAL_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:'00Z'");
  // format for the expected timestamp in a query response
  private final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
  private DateTime dtFirst;            // timestamp of 1st event
  private DateTime dtLast;             // timestamp of last event
  private DateTime dtGroupBy;          // timestamp for expected response for groupBy query

  @Inject
  ServerDiscoveryFactory factory;
  @Inject
  @Global
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  @Test
  public void testRealtimeIndexTask() throws Exception
  {
    LOG.info("Starting test: ITRealtimeIndexTaskTest");
    try {
      // the task will run for 3 minutes and then shutdown itself
      String task = setShutOffTime(
          getTaskAsString(REALTIME_TASK_RESOURCE),
          new DateTime(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(3))
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
      InputStream is = ITRealtimeIndexTaskTest.class.getResourceAsStream(REALTIME_QUERIES_RESOURCE);
      if (null == is) {
        throw new ISE("could not open query file: %s", REALTIME_QUERIES_RESOURCE);
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

  private String setShutOffTime(String taskAsString, DateTime time)
  {
    return taskAsString.replace("#SHUTOFFTIME", time.toString());
  }

  public void postEvents() throws Exception
  {
    DateTimeZone zone = DateTimeZone.forID("UTC");
    final ServerDiscoverySelector eventReceiverSelector = factory.createSelector(EVENT_RECEIVER_SERVICE_NAME);
    eventReceiverSelector.start();
    BufferedReader reader = null;
    InputStreamReader isr = null;
    try {
      isr = new InputStreamReader(ITRealtimeIndexTaskTest.class.getResourceAsStream(EVENT_DATA_FILE));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    try {
      reader = new BufferedReader(isr);
      ServerDiscoveryUtil.waitUntilInstanceReady(eventReceiverSelector, "Event Receiver");
      // Use the host from the config file and the port announced in zookeeper
      String host = config.getMiddleManagerHost() + ":" + eventReceiverSelector.pick().getPort();
      LOG.info("Event Receiver Found at host [%s]", host);
      EventReceiverFirehoseTestClient client = new EventReceiverFirehoseTestClient(
          host,
          EVENT_RECEIVER_SERVICE_NAME,
          jsonMapper,
          httpClient,
          smileMapper
      );
      // there are 22 lines in the file
      int i = 1;
      DateTime dt = new DateTime(zone);  // timestamp used for sending each event
      dtFirst = dt;                      // timestamp of 1st event
      dtLast = dt;                       // timestamp of last event
      String line;
      while ((line = reader.readLine()) != null) {
        if (i == 15) { // for the 15th line, use a time before the window
          dt = dt.minusMinutes(10);
        } else if (i == 16) { // remember this time to use in the expected response from the groupBy query
          dtGroupBy = dt;
        } else if (i == 18) { // use a time 6 seconds ago so it will be out of order
          dt = dt.minusSeconds(6);
        }
        String event = line.replace(TIME_PLACEHOLDER, EVENT_FMT.print(dt));
        LOG.info("sending event: [%s]\n", event);
        Collection<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        events.add(
            (Map<String, Object>) this.jsonMapper.readValue(
                event, new TypeReference<Map<String, Object>>()
                {
                }
            )
        );
        int eventsPosted = client.postEvents(events, this.jsonMapper, MediaType.APPLICATION_JSON);
        if (eventsPosted != events.size()) {
          throw new ISE("Event not posted");
        }

        try {
          Thread.sleep(DELAY_BETWEEN_EVENTS_SECS * 1000);
        }
        catch (InterruptedException ex) { /* nothing */ }
        dtLast = dt;
        dt = new DateTime(zone);
        i++;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      reader.close();
      eventReceiverSelector.stop();
    }
  }

  private String getRouterURL()
  {
    return String.format(
        "http://%s/druid/v2?pretty",
        config.getRouterHost()
    );
  }

}
