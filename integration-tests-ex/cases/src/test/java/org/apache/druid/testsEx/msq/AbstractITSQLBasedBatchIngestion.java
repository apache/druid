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

package org.apache.druid.testsEx.msq;

import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.testsEx.indexer.AbstractITBatchIndexTest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class AbstractITSQLBasedBatchIngestion
{
  public static final Logger LOG = new Logger(TestQueryHelper.class);
  @Inject
  private MsqTestQueryHelper msqHelper;

  @Inject
  protected TestQueryHelper queryHelper;

  @Inject
  private DataLoaderHelper dataLoaderHelper;

  protected String getFileWithFormatFromDir(String dir, String format) throws URISyntaxException
  {
    File[] file = (new File(getClass().getResource(dir).toURI())).listFiles(pathname -> {
      String name = pathname.getName();
      return name.endsWith(format) && pathname.isFile();
    });
    return dir + '/' + file[0].getName();
  }

  protected String getSqlStringFromDir(String dir, String datasource) throws URISyntaxException
  {
    String filePath = getFileWithFormatFromDir(dir, ".sql");
    String sqlString;
    try {
      InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(filePath);
      sqlString = IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new ISE(e, "could not read query file: %s", filePath);
    }

    sqlString = StringUtils.replace(
        sqlString,
        "%%DATASOURCE%%",
        datasource
    );

    return sqlString;
  }

  protected void doTestQuery(String dir, String dataSource) throws URISyntaxException
  {
    String queryFilePath = getFileWithFormatFromDir(dir, ".json");
    try {
      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryFilePath);
        queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", queryFilePath);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          dataSource
      );
      queryHelper.testQueriesFromString(queryResponseTemplate);

    }
    catch (Exception e) {
      LOG.error(e, "Error while running test query");
      throw new RuntimeException(e);
    }
  }

  protected void runMSQTaskandTestQueries(String relativePath, String datasource, Map<String, Object> msqContext) throws Exception
  {
    LOG.info("Starting MSQ test for [%s]", relativePath);

    String sqlTask = getSqlStringFromDir(relativePath, datasource);

    LOG.info("SqlTask - \n %s", sqlTask);

    // Submit the tasks and wait for the datasource to get loaded
    msqHelper.submitMsqTaskAndWaitForCompletion(
        sqlTask,
        msqContext
    );

    dataLoaderHelper.waitUntilDatasourceIsReady(datasource);
    doTestQuery(relativePath, datasource);
  }
}
