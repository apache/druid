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

package org.apache.druid.testing.embedded.indexer;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;

public class ITTransformTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK_WITH_INPUT_SOURCE = "/indexer/wikipedia_index_task_with_inputsource_transform.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries_with_transform.json";

  private static final String REINDEX_TASK_WITH_DRUID_INPUT_SOURCE = "/indexer/wikipedia_reindex_druid_input_source_task_with_transforms.json";
  private static final String REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_reindex_queries_with_transforms.json";

  @Test
  public void testIndexAndReIndexWithTransformSpec() throws IOException
  {
    final String indexDatasource = dataSource;
    final String reindexDatasourceWithDruidInputSource = EmbeddedClusterApis.createTestDatasourceName();

    try (
        final Closeable ignored1 = unloader(indexDatasource);
        final Closeable ignored2 = unloader(reindexDatasourceWithDruidInputSource)
    ) {
      doIndexTest(
          indexDatasource,
          INDEX_TASK_WITH_INPUT_SOURCE,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
      doReindexTest(
          indexDatasource,
          reindexDatasourceWithDruidInputSource,
          REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
    }
  }
}
