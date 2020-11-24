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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.curator.discovery.ServerDiscoveryFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = TestNGGroup.QUERY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITBroadcastJoinQueryTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITBroadcastJoinQueryTest.class);
  private static final String BROADCAST_JOIN_TASK = "/indexer/broadcast_join_index_task.json";
  private static final String BROADCAST_JOIN_METADATA_QUERIES_RESOURCE = "/queries/broadcast_join_metadata_queries.json";
  private static final String BROADCAST_JOIN_QUERIES_RESOURCE = "/queries/broadcast_join_queries.json";
  private static final String BROADCAST_JOIN_DATASOURCE = "broadcast_join_wikipedia_test";


  @Inject
  ServerDiscoveryFactory factory;

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @Inject
  SqlTestQueryHelper queryHelper;

  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  @Test
  public void testBroadcastJoin() throws Exception
  {
    final Closer closer = Closer.create();
    try {
      closer.register(unloader(BROADCAST_JOIN_DATASOURCE));

      // prepare for broadcast
      coordinatorClient.postLoadRules(
          BROADCAST_JOIN_DATASOURCE,
          ImmutableList.of(new ForeverBroadcastDistributionRule())
      );

      // load the data
      String taskJson = replaceJoinTemplate(getResourceAsString(BROADCAST_JOIN_TASK), BROADCAST_JOIN_DATASOURCE);
      String taskId = indexer.submitTask(taskJson);

      ITRetryUtil.retryUntilTrue(
          () -> coordinatorClient.areSegmentsLoaded(BROADCAST_JOIN_DATASOURCE), "broadcast segment load"
      );

      // query metadata until druid schema is refreshed and datasource is available joinable
      ITRetryUtil.retryUntilTrue(
          () -> {
            try {
              queryHelper.testQueriesFromString(
                  queryHelper.getQueryURL(config.getRouterUrl()),
                  replaceJoinTemplate(
                      getResourceAsString(BROADCAST_JOIN_METADATA_QUERIES_RESOURCE),
                      BROADCAST_JOIN_DATASOURCE
                  )
              );
              return true;
            }
            catch (Exception ex) {
              return false;
            }
          },
          "waiting for SQL metadata refresh"
      );

      // now do some queries
      queryHelper.testQueriesFromString(
          queryHelper.getQueryURL(config.getRouterUrl()),
          replaceJoinTemplate(getResourceAsString(BROADCAST_JOIN_QUERIES_RESOURCE), BROADCAST_JOIN_DATASOURCE)
      );
    }
    finally {
      closer.close();
    }
  }

  private static String replaceJoinTemplate(String template, String joinDataSource)
  {
    return StringUtils.replace(
        StringUtils.replace(template, "%%JOIN_DATASOURCE%%", joinDataSource),
        "%%REGULAR_DATASOURCE%%",
        ITWikipediaQueryTest.WIKIPEDIA_DATA_SOURCE
    );
  }
}
