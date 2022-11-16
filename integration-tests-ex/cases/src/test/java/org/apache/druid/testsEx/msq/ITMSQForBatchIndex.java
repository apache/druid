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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqSqlTasksWithTestQueries;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.testsEx.categories.MultiStageQuery;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.AbstractITBatchIndexTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@RunWith(DruidTestRunner.class)
 @Category(MultiStageQuery.class)
 public class ITMSQForBatchIndex
 {
   public static final Logger LOG = new Logger(TestQueryHelper.class);
   @Inject
   private MsqTestQueryHelper msqHelper;

   @Inject
   protected TestQueryHelper queryHelper;

   @Inject
   private IntegrationTestingConfig config;

   @Inject
   private ObjectMapper jsonMapper;

   @Inject
   private DataLoaderHelper dataLoaderHelper;

   @Inject
   private CoordinatorResourceTestClient coordinatorClient;

   private static final String BATCH_INDEX_TASKS_DIR = "/multi-stage-query/batch-index/";

   protected void doTestQuery(String dataSource, String queryFilePath)
   {
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
       LOG.error(e, "Error while testing");
       throw new RuntimeException(e);
     }
   }

   @Test
   public void testMsqIngestionForBatchIndexTasks() throws Exception
   {
     File[] files = (new File(getClass().getResource(BATCH_INDEX_TASKS_DIR).toURI())).listFiles();
     LOG.info(Arrays.toString(files));
     String datasource = "dst";
     for (int i=1; i<files.length; i++) {
       LOG.info("Starting MSQ test for [%s]", files[i]);
       MsqSqlTasksWithTestQueries batchSqlTasksAndQueries =
           jsonMapper.readValue(
               TestQueryHelper.class.getResourceAsStream(BATCH_INDEX_TASKS_DIR + files[i].getName()),
               new TypeReference<MsqSqlTasksWithTestQueries>()
               {
               }
           );
       // Clear up the datasource from the previous runs
       coordinatorClient.unloadSegmentsForDataSource(datasource + files[i].getName());

       String sqlTask = StringUtils.replace(
           batchSqlTasksAndQueries.getSqlTask(),
           "%%DATASOURCE%%",
           datasource + files[i].getName()
       );

       LOG.info("SqlTask - \n %s", sqlTask);

       // Submit the tasks and wait for the datasource to get loaded
       SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTask(sqlTask);

       LOG.info("Sql Task submitted with task Id - %s", sqlTaskStatus.getTaskId());

       if (sqlTaskStatus.getState().isFailure()) {
         Assert.fail(StringUtils.format(
            "Unable to start the task successfully.\nPossible exception: %s",
            sqlTaskStatus.getError()
         ));
       }
       msqHelper.pollTaskIdForCompletion(sqlTaskStatus.getTaskId());
       dataLoaderHelper.waitUntilDatasourceIsReady(datasource + files[i].getName());
       String testQueriesFilePath = batchSqlTasksAndQueries.getTestQueries();
       doTestQuery(datasource + files[i].getName(), testQueriesFilePath);
     }
   }
 }
