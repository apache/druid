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

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

@Test(groups = TestNGGroup.INPUT_SOURCE)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITHttpInputSourceTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_http_inputsource_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_http_inputsource_queries.json";

  public void doTest() throws IOException
  {
    final String indexDatasource = "wikipedia_http_inputsource_test_" + UUID.randomUUID();
    try (final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix())) {
      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
    }
  }
}
