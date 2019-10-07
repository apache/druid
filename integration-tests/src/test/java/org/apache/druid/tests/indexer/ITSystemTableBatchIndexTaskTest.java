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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;

@Test(groups = TestNGGroup.BATCH_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITSystemTableBatchIndexTaskTest extends AbstractITBatchIndexTest
{
  private static final Logger LOG = new Logger(ITSystemTableBatchIndexTaskTest.class);
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String SYSTEM_QUERIES_RESOURCE = "/indexer/sys_segment_batch_index_queries.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";

  @Test
  public void testIndexData() throws Exception
  {
    LOG.info("Starting batch index sys table queries");
    try (
        final Closeable ignored = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix())
    ) {
      doIndexTestSqlTest(
          INDEX_DATASOURCE,
          INDEX_TASK,
          SYSTEM_QUERIES_RESOURCE
      );
    }
  }
}
