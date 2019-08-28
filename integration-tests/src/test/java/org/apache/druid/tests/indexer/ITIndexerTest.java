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

import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;

@Test(groups = TestNGGroup.BATCH_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITIndexerTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";

  private static final String REINDEX_TASK = "/indexer/wikipedia_reindex_task.json";
  private static final String REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_reindex_queries.json";
  private static final String REINDEX_DATASOURCE = "wikipedia_reindex_test";

  @Test
  public void testIndexData() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
        final Closeable ignored2 = unloader(REINDEX_DATASOURCE + config.getExtraDatasourceNameSuffix())
    ) {
      doIndexTest(
          INDEX_DATASOURCE,
          INDEX_TASK,
          INDEX_QUERIES_RESOURCE,
          false
      );
      doReindexTest(
          INDEX_DATASOURCE,
          REINDEX_DATASOURCE,
          REINDEX_TASK,
          REINDEX_QUERIES_RESOURCE
      );
    }
  }
}
