/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.tests.indexer;

import com.beust.jcommander.internal.Lists;
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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITUnionQueryTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITUnionQueryTest.class);
  private static final String REALTIME_TASK_RESOURCE = "/indexer/wikipedia_realtime_index_task.json";
  private static final String EVENT_RECEIVER_SERVICE_PREFIX = "eventReceiverServiceName";
  private static final String UNION_DATA_FILE = "/indexer/wikipedia_index_data.json";
  private static final String UNION_QUERIES_RESOURCE = "/indexer/union_queries.json";
  private static final String UNION_DATASOURCE = "wikipedia_index_test";

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
    final int numTasks = 4;

    try {
      // Load 4 datasources with same dimensions
      String task = setShutOffTime(
          getTaskAsString(REALTIME_TASK_RESOURCE),
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

      // sleep for a while to let the events ingested
      TimeUnit.SECONDS.sleep(5);

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
            60000,
            10,
            "Real-time generated segments loaded"
        );
      }
      // run queries on historical nodes
      this.queryHelper.testQueriesFromFile(UNION_QUERIES_RESOURCE, 2);

    }
    catch (Exception e) {
      e.printStackTrace();
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
          httpClient
      );
      client.postEventsFromFile(UNION_DATA_FILE);
    }
    finally {
      eventReceiverSelector.stop();
    }
  }
}
