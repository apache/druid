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

package io.druid.tests.hadoop;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import io.druid.tests.indexer.AbstractIndexerTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITHadoopIndexTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITHadoopIndexTest.class);
  private static final String BATCH_TASK = "/hadoop/batch_hadoop_indexer.json";
  private static final String BATCH_QUERIES_RESOURCE = "/hadoop/batch_hadoop_queries.json";
  private static final String BATCH_DATASOURCE = "batchHadoop";
  private boolean dataLoaded = false;

  @Inject
  private IntegrationTestingConfig config;

  @BeforeClass
  public void beforeClass()
  {
    loadData(config.getProperty("hadoopTestDir") + "/batchHadoop1");
    dataLoaded = true;
  }

  @Test
  public void testHadoopIndex() throws Exception
  {
    queryHelper.testQueriesFromFile(BATCH_QUERIES_RESOURCE, 2);
  }

  private void loadData(String hadoopDir)
  {
    String indexerSpec = "";

    try {
      LOG.info("indexerFile name: [%s]", BATCH_TASK);
      indexerSpec = getTaskAsString(BATCH_TASK);
      indexerSpec = indexerSpec.replaceAll("%%HADOOP_TEST_PATH%%", hadoopDir);
    }
    catch (Exception e) {
      LOG.error("could not read and modify indexer file: %s", e.getMessage());
      throw Throwables.propagate(e);
    }

    try {
      final String taskID = indexer.submitTask(indexerSpec);
      LOG.info("TaskID for loading index task %s", taskID);
      indexer.waitUntilTaskCompletes(taskID, 60000, 20);
      RetryUtil.retryUntil(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              return coordinator.areSegmentsLoaded(BATCH_DATASOURCE);

            }
          },
          true,
          20000,
          10,
          "Segment-Load-Task-" + taskID
      );
    }
    catch (Exception e) {
      LOG.error("data could not be loaded: %s", e.getMessage());
      throw Throwables.propagate(e);
    }
  }

  @AfterClass
  public void afterClass()
  {
    if (dataLoaded) {
      try {
        unloadAndKillData(BATCH_DATASOURCE);
      }
      catch (Exception e) {
        LOG.warn(e, "exception while removing segments");
      }
    }
  }
}
