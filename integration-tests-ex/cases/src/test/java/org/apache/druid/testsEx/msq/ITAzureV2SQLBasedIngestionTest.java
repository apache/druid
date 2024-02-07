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

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.AzureDeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.AbstractAzureInputSourceParallelIndexTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * IMPORTANT:
 * To run this test, you must set the following env variables in the build environment -
 * <p>
 * The AZURE account, key and container should be set in AZURE_ACCOUNT, AZURE_KEY and AZURE_CONTAINER respectively.
 * <p>
 * <a href="https://druid.apache.org/docs/latest/development/extensions-core/azure.html">Azure Deep Storage setup in druid</a>
 */

@RunWith(DruidTestRunner.class)
@Category(AzureDeepStorage.class)
public class ITAzureV2SQLBasedIngestionTest extends AbstractAzureInputSourceParallelIndexTest
{
  private static final String CLOUD_INGEST_SQL = "/multi-stage-query/wikipedia_cloud_index_msq.sql";
  private static final String INDEX_QUERIES_FILE = "/multi-stage-query/wikipedia_index_queries.json";

  @Test
  @Parameters(method = "resources")
  @TestCaseName("Test_{index} ({0})")
  public void testSQLBasedBatchIngestion(Pair<String, List<?>> azureStorageInputSource)
  {
    doMSQTest(azureStorageInputSource, CLOUD_INGEST_SQL, INDEX_QUERIES_FILE, "azureStorage");
  }
}
