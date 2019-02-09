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
import org.apache.druid.testing.utils.RetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class AbstractITBatchIndexTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(AbstractITBatchIndexTest.class);

  @Inject
  IntegrationTestingConfig config;
  @Inject
  protected SqlTestQueryHelper sqlQueryHelper;

  @Inject
  ClientInfoResourceTestClient clientInfoResourceTestClient;

  void doIndexTestTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath,
      boolean waitForMultipleVersions
  ) throws IOException
  {
    final String fullDatasourceName = dataSource + config.getExtraDatasourceNameSuffix();
    final String taskSpec = StringUtils.replace(
        getTaskAsString(indexTaskFilePath),
        "%%DATASOURCE%%",
        fullDatasourceName
    );

    submitTaskAndWait(taskSpec, fullDatasourceName, waitForMultipleVersions);
    try {

      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryFilePath);
        queryResponseTemplate = IOUtils.toString(is, "UTF-8");
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", queryFilePath);
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

  void doReindexTest(
      String baseDataSource,
      String reindexDataSource,
      String reindexTaskFilePath,
      String queryFilePath
  ) throws IOException
  {
    final String fullBaseDatasourceName = baseDataSource + config.getExtraDatasourceNameSuffix();
    final String fullReindexDatasourceName = reindexDataSource + config.getExtraDatasourceNameSuffix();

    String taskSpec = StringUtils.replace(
        getTaskAsString(reindexTaskFilePath),
        "%%DATASOURCE%%",
        fullBaseDatasourceName
    );

    taskSpec = StringUtils.replace(
        taskSpec,
        "%%REINDEX_DATASOURCE%%",
        fullReindexDatasourceName
    );

    submitTaskAndWait(taskSpec, fullReindexDatasourceName, false);
    try {
      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryFilePath);
        queryResponseTemplate = IOUtils.toString(is, "UTF-8");
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", queryFilePath);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullBaseDatasourceName
      );

      queryHelper.testQueriesFromString(queryResponseTemplate, 2);
      // verify excluded dimension is not reIndexed
      final List<String> dimensions = clientInfoResourceTestClient.getDimensions(
          fullReindexDatasourceName,
          "2013-08-31T00:00:00.000Z/2013-09-10T00:00:00.000Z"
      );
      Assert.assertFalse("dimensions : " + dimensions, dimensions.contains("robot"));
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  void doIndexTestSqlTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath
  )
  {
    submitTaskAndWait(indexTaskFilePath, dataSource, false);
    try {
      sqlQueryHelper.testQueriesFromFile(queryFilePath, 2);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  private void submitTaskAndWait(String taskSpec, String dataSourceName, boolean waitForMultipleVersions)
  {
    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    // ITParallelIndexTest does a second round of ingestion to replace segements in an existing
    // data source. For that second round we need to make sure the coordinator actually learned
    // about the new segments befor waiting for it to report that all segments are loaded; otherwise
    // this method could return too early because the coordinator is merely reporting that all the
    // original segments have loaded. Waiting for the coordinator know about more than one version
    // worth of segments is a simple way to differentiate between only knowing about the first segments
    // and knowing about the second ones too.
    if (waitForMultipleVersions) {
      RetryUtil.retryUntilTrue(
          () -> coordinator.getSegmentVersions(dataSourceName).size() > 1, "See multiple versions"
      );
    }

    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(dataSourceName), "Segment Load"
    );
  }
}
