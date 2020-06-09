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
  private static final String INPUT_SOURCE_PATHS_KEY = "paths";

  @DataProvider
  public static Object[][] resources()
  {
    return new Object[][]{
        {new Pair<>(INPUT_SOURCE_PATHS_KEY,
                    "hdfs://druid-it-hadoop:9000/batch_index%%FOLDER_SUFFIX%%"
        )},
        {new Pair<>(INPUT_SOURCE_PATHS_KEY,
                    ImmutableList.of(
                        "hdfs://druid-it-hadoop:9000/batch_index%%FOLDER_SUFFIX%%"
                    )
        )},
        {new Pair<>(INPUT_SOURCE_PATHS_KEY,
                    ImmutableList.of(
                        "hdfs://druid-it-hadoop:9000/batch_index%%FOLDER_SUFFIX%%/wikipedia_index_data1%%FILE_EXTENSION%%",
                        "hdfs://druid-it-hadoop:9000/batch_index%%FOLDER_SUFFIX%%/wikipedia_index_data2%%FILE_EXTENSION%%",
                        "hdfs://druid-it-hadoop:9000/batch_index%%FOLDER_SUFFIX%%/wikipedia_index_data3%%FILE_EXTENSION%%"
                    )
        )}
    };
  }

  void doTest(Pair<String, List> hdfsInputSource, InputFormatDetails inputFormatDetails) throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
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
          spec = StringUtils.replace(
              spec,
              "%%INPUT_FORMAT_TYPE%%",
              inputFormatDetails.getInputFormatType()
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_VALUE%%",
              jsonMapper.writeValueAsString(hdfsInputSource.rhs)
          );
          spec = StringUtils.replace(
              spec,
              "%%FOLDER_SUFFIX%%",
              inputFormatDetails.getFolderSuffix()
          );
          spec = StringUtils.replace(
              spec,
              "%%FILE_EXTENSION%%",
              inputFormatDetails.getFileExtension()
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
          hdfsPropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}
