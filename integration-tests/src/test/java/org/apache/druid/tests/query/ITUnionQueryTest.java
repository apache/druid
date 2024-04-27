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

package org.apache.druid.tests.query;

import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.curator.discovery.ServerDiscoveryFactory;
import org.apache.druid.curator.discovery.ServerDiscoverySelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.EventReceiverFirehoseTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.ServerDiscoveryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractITBatchIndexTest;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = {TestNGGroup.QUERY, TestNGGroup.CENTRALIZED_DATASOURCE_SCHEMA})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITUnionQueryTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITUnionQueryTest.class);
  private static final String UNION_TASK_RESOURCE = "/indexer/wikipedia_union_index_task.json";
  private static final String EVENT_RECEIVER_SERVICE_PREFIX = "eventReceiverServiceName";
  private static final String UNION_DATA_FILE = "/data/union_query/wikipedia_index_data.json";
  private static final String UNION_QUERIES_RESOURCE = "/queries/union_queries.json";
  private static final String UNION_DATASOURCE = "wikipedia_index_test";

  @Inject
  ServerDiscoveryFactory factory;

  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  private String fullDatasourceName;

  @BeforeSuite
  public void setFullDatasourceName()
  {
    fullDatasourceName = UNION_DATASOURCE + config.getExtraDatasourceNameSuffix();
  }

  @Test
  public void testUnionQuery() throws IOException
  {
    final int numTasks = 3;
    final Closer closer = Closer.create();
    for (int i = 0; i < numTasks; i++) {
      closer.register(unloader(fullDatasourceName + i));
    }
    try {
      // Load 3 datasources with same dimensions
      String task = setShutOffTime(
          getResourceAsString(UNION_TASK_RESOURCE),
          DateTimes.utc(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(3))
      );
      List<String> taskIDs = new ArrayList<>();
      for (int i = 0; i < numTasks; i++) {
        taskIDs.add(
            indexer.submitTask(
                withServiceName(
                    withDataSource(task, fullDatasourceName + i),
                    EVENT_RECEIVER_SERVICE_PREFIX + i
                )
            )
        );
      }
      for (int i = 0; i < numTasks; i++) {
        postEvents(i);
      }

      // wait until all events are ingested
      ITRetryUtil.retryUntil(
          () -> {
            for (int i = 0; i < numTasks; i++) {
              final int countRows = queryHelper.countRows(
                  fullDatasourceName + i,
                  Intervals.of("2013-08-31/2013-09-01"),
                  name -> new LongSumAggregatorFactory(name, "count")
              );

              // there are 10 rows, but query only covers the first 5
              if (countRows < 5) {
                LOG.warn("%d events have been ingested to %s so far", countRows, fullDatasourceName + i);
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

      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(UNION_QUERIES_RESOURCE);
        queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", UNION_QUERIES_RESOURCE);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullDatasourceName
      );

      this.queryHelper.testQueriesFromString(queryResponseTemplate);

      // wait for the task to complete
      for (int i = 0; i < numTasks; i++) {
        indexer.waitUntilTaskCompletes(taskIDs.get(i));
      }
      // task should complete only after the segments are loaded by historical node
      for (int i = 0; i < numTasks; i++) {
        final int taskNum = i;
        ITRetryUtil.retryUntil(
            () -> coordinator.areSegmentsLoaded(fullDatasourceName + taskNum),
            true,
            10000,
            10,
            "Real-time generated segments loaded"
        );
      }
      // run queries on historical nodes
      this.queryHelper.testQueriesFromString(queryResponseTemplate);

    }
    catch (Throwable e) {
      throw closer.rethrow(e);
    }
    finally {
      closer.close();
    }
  }

  private String setShutOffTime(String taskAsString, DateTime time)
  {
    return StringUtils.replace(taskAsString, "#SHUTOFFTIME", time.toString());
  }

  private String withDataSource(String taskAsString, String dataSource)
  {
    return StringUtils.replace(taskAsString, "%%DATASOURCE%%", dataSource);
  }

  private String withServiceName(String taskAsString, String serviceName)
  {
    return StringUtils.replace(taskAsString, EVENT_RECEIVER_SERVICE_PREFIX, serviceName);
  }

  private void postEvents(int id) throws Exception
  {
    final ServerDiscoverySelector eventReceiverSelector = factory.createSelector(EVENT_RECEIVER_SERVICE_PREFIX + id);
    eventReceiverSelector.start();
    try {
      ServerDiscoveryUtil.waitUntilInstanceReady(eventReceiverSelector, "Event Receiver");
      // Access the docker VM mapped host and port instead of service announced in zookeeper
      String host = config.getMiddleManagerHost() + ":" + eventReceiverSelector.pick().getPort();

      LOG.info("Event Receiver Found at host [%s]", host);

      LOG.info("Checking worker /status/health for [%s]", host);
      ITRetryUtil.retryUntilTrue(
          () -> {
            try {
              StatusResponseHolder response = httpClient.go(
                  new Request(HttpMethod.GET, new URL(StringUtils.format("https://%s/status/health", host))),
                  StatusResponseHandler.getInstance()
              ).get();
              return response.getStatus().equals(HttpResponseStatus.OK);
            }
            catch (Throwable e) {
              LOG.error(e, "");
              return false;
            }
          },
          StringUtils.format("Checking /status/health for worker [%s]", host)
      );
      LOG.info("Finished checking worker /status/health for [%s], success", host);

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
