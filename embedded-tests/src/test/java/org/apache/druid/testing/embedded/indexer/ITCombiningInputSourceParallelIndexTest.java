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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Function;

public class ITCombiningInputSourceParallelIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  private static final String COMBINING_INDEX_TASK = "/indexer/wikipedia_combining_input_source_index_parallel_task.json";
  private static final String COMBINING_QUERIES_RESOURCE = "/indexer/wikipedia_combining_input_source_index_queries.json";

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // Testing the legacy config from https://github.com/apache/druid/pull/10267
    // Can remove this when the flag is no longer needed
    cluster.addCommonProperty("druid.indexer.task.ignoreTimestampSpecForDruidInputSource", "true");
  }

  @Test
  public void testIndexData() throws Exception
  {
    final String indexDatasource = dataSource;
    final String combiningDatasource = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());
    Map<String, Object> inputFormatMap = new ImmutableMap
        .Builder<String, Object>()
        .put("type", "json")
        .build();
    try (
        final Closeable ignored1 = unloader(indexDatasource);
        final Closeable ignored2 = unloader(combiningDatasource);
    ) {
      final Function<String, String> combiningInputSourceSpecTransform = spec -> {
        try {
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_FILTER%%",
              "tiny_wiki*"
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_BASE_DIR%%",
              getDataDirectory() + "/json"
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_FORMAT%%",
              jsonMapper.writeValueAsString(inputFormatMap)
          );
          spec = StringUtils.replace(
              spec,
              "%%APPEND_TO_EXISTING%%",
              jsonMapper.writeValueAsString(false)
          );
          spec = StringUtils.replace(
              spec,
              "%%DROP_EXISTING%%",
              jsonMapper.writeValueAsString(false)
          );
          spec = StringUtils.replace(
              spec,
              "%%FORCE_GUARANTEED_ROLLUP%%",
              jsonMapper.writeValueAsString(false)
          );
          spec = StringUtils.replace(
              spec,
              "%%COMBINING_DATASOURCE%%",
              indexDatasource
          );
          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          combiningInputSourceSpecTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
      doIndexTest(
          combiningDatasource,
          COMBINING_INDEX_TASK,
          combiningInputSourceSpecTransform,
          COMBINING_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
    }
  }
}
