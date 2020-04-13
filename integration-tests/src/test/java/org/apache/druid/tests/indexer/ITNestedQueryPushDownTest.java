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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.ClientInfoResourceTestClient;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Test(groups = TestNGGroup.QUERY)
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

  private String fullDatasourceName;

  @BeforeSuite
  public void setFullDatasourceName()
  {
    fullDatasourceName = WIKITICKER_DATA_SOURCE + config.getExtraDatasourceNameSuffix();
  }

  @Test
  public void testIndexData()
  {
    try {
      loadData();

      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(WIKITICKER_QUERIES_RESOURCE);
        queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", WIKITICKER_QUERIES_RESOURCE);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullDatasourceName
      );

      queryHelper.testQueriesFromString(queryResponseTemplate, 2);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  private void loadData() throws Exception
  {
    String taskSpec = getResourceAsString(WIKITICKER_INDEX_TASK);
    taskSpec = StringUtils.replace(taskSpec, "%%DATASOURCE%%", fullDatasourceName);
    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName), "Segment Load"
    );
  }
}
