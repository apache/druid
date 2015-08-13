/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.tests.indexer;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.clients.EventReceiverFirehoseTestClient;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import io.druid.testing.utils.ServerDiscoveryUtil;
import org.joda.time.DateTime;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Steps
 * 1) Submit a RealtimeIndexTask
 * 2) Load Data using EventReceiverFirehose
 * 3) Runs queries and verifies that the ingested data is available for queries
 * 4) Waits for handover of the segment to historical node
 * 5) Queries data from historical node and verifies handover
 * 6) Removes and Delete the created Data Segment
 */
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITRealtimeIndexTaskTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITRealtimeIndexTaskTest.class);
  private static final String REALTIME_TASK_RESOURCE = "/indexer/wikipedia_realtime_index_task.json";
  private static final String EVENT_RECEIVER_SERVICE_NAME = "eventReceiverServiceName";
  private static final String EVENT_DATA_FILE = "/indexer/wikipedia_index_data.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";
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
    try {
      // the task will run for 3 minutes and then shutdown itself
      String task = setShutOffTime(
          getTaskAsString(REALTIME_TASK_RESOURCE),
          new DateTime(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2))
      );
      String taskID = indexer.submitTask(task);
      postEvents();

      // sleep for a while to let the events ingested
      TimeUnit.SECONDS.sleep(5);

      // should hit the queries on realtime task
      this.queryHelper.testQueriesFromFile(INDEX_QUERIES_RESOURCE, 2);
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

      // run queries on historical nodes
      this.queryHelper.testQueriesFromFile(INDEX_QUERIES_RESOURCE, 2);
    }
    catch (Exception e) {
      e.printStackTrace();
      Throwables.propagate(e);
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
    final ServerDiscoverySelector eventReceiverSelector = factory.createSelector(EVENT_RECEIVER_SERVICE_NAME);
    eventReceiverSelector.start();
    try {
      ServerDiscoveryUtil.waitUntilInstanceReady(eventReceiverSelector, "Event Receiver");
      // Access the docker VM mapped host and port instead of service announced in zookeeper
      String host = config.getMiddleManagerHost() + ":" + eventReceiverSelector.pick().getPort();
      LOG.info("Event Receiver Found at host %s", host);
      EventReceiverFirehoseTestClient client = new EventReceiverFirehoseTestClient(
          host,
          EVENT_RECEIVER_SERVICE_NAME,
          jsonMapper,
          httpClient,
          smileMapper
      );
      client.postEventsFromFile(EVENT_DATA_FILE);
    }
    finally {
      eventReceiverSelector.stop();
    }
  }
}
