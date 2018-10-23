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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.ClientInfoResourceTestClient;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.RetryUtil;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITNestedQueryPushDownTest extends AbstractIndexerTest
{
  private static final String WIKITICKER_DATA_SOURCE = "wikiticker";
  private static final String WIKITICKER_INDEX_TASK = "/indexer/wikiticker_index_task.json";
  private static final String WIKITICKER_QUERIES_RESOURCE = "/queries/nestedquerypushdown_queries.json";

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;
  @Inject
  private TestQueryHelper queryHelper;

  private static final Logger LOG = new Logger(ITNestedQueryPushDownTest.class);

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  ClientInfoResourceTestClient clientInfoResourceTestClient;
  @Test
  public void testIndexData()
  {
    try {
      loadData();
      queryHelper.testQueriesFromFile(WIKITICKER_QUERIES_RESOURCE, 2);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw Throwables.propagate(e);
    }
  }

  private void loadData() throws Exception
  {
    final String taskID = indexer.submitTask(getTaskAsString(WIKITICKER_INDEX_TASK));
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);
    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(WIKITICKER_DATA_SOURCE), "Segment Load"
    );
  }
}
