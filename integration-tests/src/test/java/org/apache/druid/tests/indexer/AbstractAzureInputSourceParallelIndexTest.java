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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.testng.annotations.DataProvider;

import java.io.Closeable;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractAzureInputSourceParallelIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_cloud_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INPUT_SOURCE_URIS_KEY = "uris";
  private static final String INPUT_SOURCE_PREFIXES_KEY = "prefixes";
  private static final String INPUT_SOURCE_OBJECTS_KEY = "objects";
  private static final String WIKIPEDIA_DATA_1 = "wikipedia_index_data1.json";
  private static final String WIKIPEDIA_DATA_2 = "wikipedia_index_data2.json";
  private static final String WIKIPEDIA_DATA_3 = "wikipedia_index_data3.json";

  @DataProvider
  public static Object[][] resources()
  {
    return new Object[][]{
        {new Pair<>(INPUT_SOURCE_URIS_KEY,
                    ImmutableList.of(
                        "azure://%%BUCKET%%/%%PATH%%" + WIKIPEDIA_DATA_1,
                        "azure://%%BUCKET%%/%%PATH%%" + WIKIPEDIA_DATA_2,
                        "azure://%%BUCKET%%/%%PATH%%" + WIKIPEDIA_DATA_3
                    )
        )},
        {new Pair<>(INPUT_SOURCE_PREFIXES_KEY,
                    ImmutableList.of(
                        "azure://%%BUCKET%%/%%PATH%%"
                    )
        )},
        {new Pair<>(INPUT_SOURCE_OBJECTS_KEY,
                    ImmutableList.of(
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_1),
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_2),
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_3)
                    )
        )}
    };
  }

  void doTest(Pair<String, List> azureInputSource) throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> azurePropsTransform = spec -> {
        try {
          String inputSourceValue = jsonMapper.writeValueAsString(azureInputSource.rhs);
          inputSourceValue = StringUtils.replace(
              inputSourceValue,
              "%%BUCKET%%",
              config.getCloudBucket()
          );
          inputSourceValue = StringUtils.replace(
              inputSourceValue,
              "%%PATH%%",
              config.getCloudPath()
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_FORMAT_TYPE%%",
              InputFormatDetails.JSON.getInputFormatType()
          );
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_TYPE%%",
              "azure"
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_KEY%%",
              azureInputSource.lhs
          );
          return StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_VALUE%%",
              inputSourceValue
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          azurePropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}
