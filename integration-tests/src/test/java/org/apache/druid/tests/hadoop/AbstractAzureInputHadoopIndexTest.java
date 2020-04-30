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

package org.apache.druid.tests.hadoop;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.tests.indexer.AbstractITBatchIndexTest;

import java.io.Closeable;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractAzureInputHadoopIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/hadoop/wikipedia_hadoop_azure_input_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  void doTest() throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> azurePropsTransform = spec -> {
        try {
          String path = StringUtils.format(
              "wasbs://%s@%s.blob.core.windows.net/",
              config.getCloudPath(),
              config.getCloudBucket()
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_PATHS%%",
              path
          );

          spec = StringUtils.replace(
              spec,
              "%%AZURE_ACCOUNT%%",
              config.getCloudBucket()
          );

          spec = StringUtils.replace(
              spec,
              "%%AZURE_KEY%%",
              config.getAzureKey()
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
          azurePropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}
