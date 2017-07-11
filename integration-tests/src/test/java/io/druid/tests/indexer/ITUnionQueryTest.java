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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.clients.EventReceiverFirehoseTestClient;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.guice.TestClient;
import io.druid.testing.utils.RetryUtil;
import io.druid.testing.utils.ServerDiscoveryUtil;
import org.joda.time.DateTime;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITUnionQueryTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITUnionQueryTest.class);
  private static final String UNION_TASK_RESOURCE = "/indexer/wikipedia_union_index_task.json";
  private static final String EVENT_RECEIVER_SERVICE_PREFIX = "eventReceiverServiceName";
  private static final String UNION_DATA_FILE = "/indexer/wikipedia_index_data.json";
  private static final String UNION_QUERIES_RESOURCE = "/indexer/union_queries.json";
  private static final String UNION_DATASOURCE = "wikipedia_index_test";

  @Inject
  ServerDiscoveryFactory factory;

  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  @Test
  public void testUnionQuery() throws Exception
  {
    final int numTasks = 3;

    try {
      // Load 4 datasources with same dimensions
      String task = setShutOffTime(
          getTaskAsString(UNION_TASK_RESOURCE),
          new DateTime(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(3))
      );
      List<String> taskIDs = Lists.newArrayList();
      for (int i = 0; i < numTasks; i++) {
        taskIDs.add(
            indexer.submitTask(
                withServiceName(
                    withDataSource(task, UNION_DATASOURCE + i),
                    EVENT_RECEIVER_SERVICE_PREFIX + i
                )
            )
        );
      }
      for (int i = 0; i < numTasks; i++) {
        postEvents(i);
      }

      // wait until all events are ingested
      RetryUtil.retryUntil(
          () -> {
            for (int i = 0; i < numTasks; i++) {
              if (queryHelper.countRows(UNION_DATASOURCE + i, "2013-08-31/2013-09-01") < 5) {
                return false;
              }
            }
            return true;
          },
          true,
          1000,
          100,
          "Waiting all events are ingested"
      );

      // should hit the queries on realtime task
      LOG.info("Running Union Queries..");
      this.queryHelper.testQueriesFromFile(UNION_QUERIES_RESOURCE, 2);

      // wait for the task to complete
      for (int i = 0; i < numTasks; i++) {
        indexer.waitUntilTaskCompletes(taskIDs.get(i));
      }
      // task should complete only after the segments are loaded by historical node
      for (int i = 0; i < numTasks; i++) {
        final int taskNum = i;
        RetryUtil.retryUntil(
            new Callable<Boolean>()
            {
              @Override
              public Boolean call() throws Exception
              {
                return coordinator.areSegmentsLoaded(UNION_DATASOURCE + taskNum);
              }
            },
            true,
            10000,
            10,
            "Real-time generated segments loaded"
        );
      }
      // run queries on historical nodes
      this.queryHelper.testQueriesFromFile(UNION_QUERIES_RESOURCE, 2);

    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw Throwables.propagate(e);
    }
    finally {
      for (int i = 0; i < numTasks; i++) {
        unloadAndKillData(UNION_DATASOURCE + i);
      }
    }

  }

  private String setShutOffTime(String taskAsString, DateTime time)
  {
    return taskAsString.replace("#SHUTOFFTIME", time.toString());
  }

  private String withDataSource(String taskAsString, String dataSource)
  {
    return taskAsString.replace(UNION_DATASOURCE, dataSource);
  }

  private String withServiceName(String taskAsString, String serviceName)
  {
    return taskAsString.replace(EVENT_RECEIVER_SERVICE_PREFIX, serviceName);
  }

  public void postEvents(int id) throws Exception
  {
    final ServerDiscoverySelector eventReceiverSelector = factory.createSelector(EVENT_RECEIVER_SERVICE_PREFIX + id);
    eventReceiverSelector.start();
    try {
      ServerDiscoveryUtil.waitUntilInstanceReady(eventReceiverSelector, "Event Receiver");
      // Access the docker VM mapped host and port instead of service announced in zookeeper
      String host = config.getMiddleManagerHost() + ":" + eventReceiverSelector.pick().getPort();

      LOG.info("Event Receiver Found at host [%s]", host);

      EventReceiverFirehoseTestClient client = new EventReceiverFirehoseTestClient(
          host,
          EVENT_RECEIVER_SERVICE_PREFIX + id,
          jsonMapper,
          httpClient,
          smileMapper
      );
      client.postEventsFromFile(UNION_DATA_FILE);
    }
    finally {
      eventReceiverSelector.stop();
    }
  }
}
