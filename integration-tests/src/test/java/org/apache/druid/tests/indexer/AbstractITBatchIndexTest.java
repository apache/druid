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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.ClientInfoResourceTestClient;
import org.apache.druid.testing.utils.RetryUtil;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;

public class AbstractITBatchIndexTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(AbstractITBatchIndexTest.class);

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ClientInfoResourceTestClient clientInfoResourceTestClient;

  void doIndexTestTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath
  ) throws IOException
  {
    submitTaskAndWait(indexTaskFilePath, dataSource);
    try {
      queryHelper.testQueriesFromFile(queryFilePath, 2);

    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  void doReindexTest(
      String reindexDataSource,
      String reindexTaskFilePath,
      String queryFilePath
  ) throws IOException
  {
    submitTaskAndWait(reindexTaskFilePath, reindexDataSource);
    try {
      queryHelper.testQueriesFromFile(queryFilePath, 2);
      // verify excluded dimension is not reIndexed
      final List<String> dimensions = clientInfoResourceTestClient.getDimensions(
          reindexDataSource,
          "2013-08-31T00:00:00.000Z/2013-09-10T00:00:00.000Z"
      );
      Assert.assertFalse("dimensions : " + dimensions, dimensions.contains("robot"));
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  private void submitTaskAndWait(String indexTaskFilePath, String dataSourceName) throws IOException
  {
    final String taskID = indexer.submitTask(getTaskAsString(indexTaskFilePath));
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(dataSourceName), "Segment Load"
    );
  }
}
