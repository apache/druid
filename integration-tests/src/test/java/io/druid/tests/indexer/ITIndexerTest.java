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
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.clients.ClientInfoResourceTestClient;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import org.junit.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Callable;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITIndexerTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITIndexerTest.class);
  private static String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static String INDEX_DATASOURCE = "wikipedia_index_test";
  private static String REINDEX_TASK = "/indexer/wikipedia_reindex_task.json";
  private static String REINDEX_DATASOURCE = "wikipedia_reindex_test";

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  ClientInfoResourceTestClient clientInfoResourceTestClient;

  @Test
  public void testIndexData() throws Exception
  {
    loadData();
    try {
      queryHelper.testQueriesFromFile(INDEX_QUERIES_RESOURCE, 2);
      reIndexData();
      queryHelper.testQueriesFromFile(INDEX_QUERIES_RESOURCE, 2);
      // verify excluded dimension is not reIndexed
      final List<String> dimensions = clientInfoResourceTestClient.getDimensions(
          REINDEX_DATASOURCE,
          "2013-08-31T00:00:00.000Z/2013-09-10T00:00:00.000Z"
      );
      Assert.assertFalse("dimensions : " + dimensions, dimensions.contains("robot"));
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw Throwables.propagate(e);
    }
    finally {
      unloadAndKillData(INDEX_DATASOURCE);
      unloadAndKillData(REINDEX_DATASOURCE);

    }

  }

  private void loadData() throws Exception
  {
    final String taskID = indexer.submitTask(getTaskAsString(INDEX_TASK));
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return coordinator.areSegmentsLoaded(INDEX_DATASOURCE);
          }
        }, "Segment Load"
    );
  }

  private void reIndexData() throws Exception
  {
    final String taskID = indexer.submitTask(getTaskAsString(REINDEX_TASK));
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return coordinator.areSegmentsLoaded(REINDEX_DATASOURCE);
          }
        }, "Segment Load"
    );
  }

}
