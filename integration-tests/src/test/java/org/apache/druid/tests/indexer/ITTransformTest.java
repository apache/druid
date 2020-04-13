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
import java.io.IOException;

@Test(groups = {TestNGGroup.BATCH_INDEX, TestNGGroup.QUICKSTART_COMPATIBLE})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITTransformTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK_WITH_FIREHOSE = "/indexer/wikipedia_index_task_with_transform.json";
  private static final String INDEX_TASK_WITH_INPUT_SOURCE = "/indexer/wikipedia_index_task_with_inputsource_transform.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries_with_transform.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";

  private static final String REINDEX_TASK = "/indexer/wikipedia_reindex_task_with_transforms.json";
  private static final String REINDEX_TASK_WITH_DRUID_INPUT_SOURCE = "/indexer/wikipedia_reindex_druid_input_source_task_with_transforms.json";
  private static final String REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_reindex_queries_with_transforms.json";
  private static final String REINDEX_DATASOURCE = "wikipedia_reindex_test";

  @Test
  public void testIndexAndReIndexWithTransformSpec() throws IOException
  {
    final String reindexDatasourceWithDruidInputSource = REINDEX_DATASOURCE + "-druidInputSource";

    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
        final Closeable ignored2 = unloader(reindexDatasourceWithDruidInputSource + config.getExtraDatasourceNameSuffix())
    ) {
      doIndexTest(
          INDEX_DATASOURCE,
          INDEX_TASK_WITH_INPUT_SOURCE,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
      doReindexTest(
          INDEX_DATASOURCE,
          reindexDatasourceWithDruidInputSource,
          REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          REINDEX_QUERIES_RESOURCE
      );
    }
  }

  @Test(enabled = false)
  public void testIndexAndReIndexUsingIngestSegmentWithTransforms() throws IOException
  {
    // TODO: re-instate this test when https://github.com/apache/druid/issues/9591 is fixed
    // Move the re-index step into testIndexAndReIndexWithTransformSpec for faster tests!
    final String reindexDatasource = REINDEX_DATASOURCE + "-testIndexData";
    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
        final Closeable ignored2 = unloader(reindexDatasource + config.getExtraDatasourceNameSuffix())
    ) {
      doIndexTest(
          INDEX_DATASOURCE,
          INDEX_TASK_WITH_INPUT_SOURCE,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
      doReindexTest(
          INDEX_DATASOURCE,
          reindexDatasource,
          REINDEX_TASK,
          REINDEX_QUERIES_RESOURCE
      );
    }
  }

  @Test(enabled = false)
  public void testIndexWithFirehoseAndTransforms() throws IOException
  {
    // TODO: re-instate this test when https://github.com/apache/druid/issues/9589 is fixed
    final String indexDatasource = INDEX_DATASOURCE + "-firehose";
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      doIndexTest(
          indexDatasource,
          INDEX_TASK_WITH_FIREHOSE,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}
