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

import com.google.inject.Inject;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.tests.indexer.AbstractITBatchIndexTest;

import java.io.Closeable;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractS3InputHadoopIndexTest extends AbstractITBatchIndexTest
{
  @Inject
  protected AWSCredentialsConfig awsCredentialsConfig;

  private static final String INDEX_TASK = "/hadoop/wikipedia_hadoop_s3_input_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  void doTest() throws Exception
  {
    final String indexDatasource = "wikipedia_hadoop_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> s3PropsTransform = spec -> {
        try {

          String path = StringUtils.format(
              "s3a://%s/%s",
              config.getCloudBucket(),
              config.getCloudPath()
          );

          spec = StringUtils.replace(
              spec,
              "%%INPUT_PATHS%%",
              path
          );

          spec = StringUtils.replace(
              spec,
              "%%AWS_ACCESS_KEY%%",
              awsCredentialsConfig.getAccessKey().getPassword()
          );

          spec = StringUtils.replace(
              spec,
              "%%AWS_SECRET_KEY%%",
              awsCredentialsConfig.getSecretKey().getPassword()
          );

          spec = StringUtils.replace(
              spec,
              "%%AWS_REGION%%",
              config.getCloudRegion()
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
          s3PropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }
  }
}
