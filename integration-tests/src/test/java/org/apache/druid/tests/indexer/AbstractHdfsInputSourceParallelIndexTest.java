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
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.testng.annotations.DataProvider;

import java.io.Closeable;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractHdfsInputSourceParallelIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_cloud_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test_" + UUID.randomUUID();
  private static final String INPUT_SOURCE_PATHS_KEY = "paths";

  @DataProvider
  public static Object[][] resources()
  {
    return new Object[][]{
        {new Pair<>(INPUT_SOURCE_PATHS_KEY,
                    "hdfs://druid-it-hadoop:9000/batch_index"
        )},
        {new Pair<>(INPUT_SOURCE_PATHS_KEY,
                    ImmutableList.of(
                        "hdfs://druid-it-hadoop:9000/batch_index"
                    )
        )},
        {new Pair<>(INPUT_SOURCE_PATHS_KEY,
                    ImmutableList.of(
                        "hdfs://druid-it-hadoop:9000/batch_index/wikipedia_index_data1.json",
                        "hdfs://druid-it-hadoop:9000/batch_index/wikipedia_index_data2.json",
                        "hdfs://druid-it-hadoop:9000/batch_index/wikipedia_index_data3.json"
                    )
        )}
    };
  }

  void doTest(Pair<String, List> hdfsInputSource) throws Exception
  {
    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> hdfsPropsTransform = spec -> {
        try {
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_TYPE%%",
              "hdfs"
          );
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_KEY%%",
              hdfsInputSource.lhs
          );
          return StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_VALUE%%",
              jsonMapper.writeValueAsString(hdfsInputSource.rhs)
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          INDEX_DATASOURCE,
          INDEX_TASK,
          hdfsPropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}
