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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractLocalInputSourceParallelIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  public void doIndexTest(InputFormatDetails inputFormatDetails) throws Exception
  {
    doIndexTest(inputFormatDetails, ImmutableMap.of());
  }

  public void doIndexTest(InputFormatDetails inputFormatDetails, @Nonnull Map<String, Object> extraInputFormatMap) throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    Map inputFormatMap = new ImmutableMap.Builder<String, Object>().putAll(extraInputFormatMap)
                                                                 .put("type", inputFormatDetails.getInputFormatType())
                                                                 .build();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> sqlInputSourcePropsTransform = spec -> {
        try {
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_FILTER%%",
              "*" + inputFormatDetails.getFileExtension()
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_BASE_DIR%%",
              "/resources/data/batch_index" + inputFormatDetails.getFolderSuffix()
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
              "%%FORCE_GUARANTEED_ROLLUP%%",
              jsonMapper.writeValueAsString(false)
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
          sqlInputSourcePropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}
