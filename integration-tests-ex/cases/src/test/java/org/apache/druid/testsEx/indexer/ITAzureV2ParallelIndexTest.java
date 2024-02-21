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

package org.apache.druid.testsEx.indexer;

import junitparams.Parameters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.AzureDeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * IMPORTANT:
 * To run this test, you must set the following env variables in the build environment -
 * DRUID_CLOUD_PATH - path inside the container where the test data files will be uploaded
 * <p>
 * The AZURE account, key and container should be set in AZURE_ACCOUNT, AZURE_KEY and AZURE_CONTAINER respectively.
 * <p>
 * <a href="https://druid.apache.org/docs/latest/development/extensions-core/azure.html">Azure Deep Storage setup in druid</a>
 */

@RunWith(DruidTestRunner.class)
@Category(AzureDeepStorage.class)
public class ITAzureV2ParallelIndexTest extends AbstractAzureInputSourceParallelIndexTest
{
  @Test
  @Parameters(method = "resources")
  public void testAzureIndexData(Pair<String, List<?>> azureInputSource) throws Exception
  {
    String dataSource = doTest(azureInputSource, new Pair<>(false, false), "azureStorage");
    AbstractAzureInputSourceParallelIndexTest.validateAzureSegmentFilesDeleted("segments" + "/" + dataSource);
  }
}
