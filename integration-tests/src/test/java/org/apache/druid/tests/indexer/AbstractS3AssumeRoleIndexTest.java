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
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.testng.Assert;
import org.testng.SkipException;

import java.io.Closeable;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractS3AssumeRoleIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK_WITH_OVERRIDE = "/indexer/wikipedia_override_credentials_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INPUT_SOURCE_OBJECTS_KEY = "objects";
  private static final String WIKIPEDIA_DATA_1 = "wikipedia_index_data1.json";
  private static final String WIKIPEDIA_DATA_2 = "wikipedia_index_data2.json";
  private static final String WIKIPEDIA_DATA_3 = "wikipedia_index_data3.json";
  private static final ImmutableList INPUT_SOURCE_OBJECTS_VALUE = ImmutableList.of
      (
          ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_1),
          ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_2),
          ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_3)
      );

  abstract boolean isSetS3OverrideCredentials();

  void doTestS3WithValidAssumeRoleAndExternalIdShouldSucceed() throws Exception
  {
    if (config.getS3AssumeRoleExternalId() == null || config.getS3AssumeRoleWithExternalId() == null) {
      throw new SkipException("S3 Assume Role and external Id must be set for this test");
    }
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> s3PropsTransform = spec -> {
        try {
          String inputSourceValue = jsonMapper.writeValueAsString(INPUT_SOURCE_OBJECTS_VALUE);
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
          ImmutableMap.Builder<String, Object> s3ConfigMap = ImmutableMap.builder();
          if (isSetS3OverrideCredentials()) {
            s3ConfigMap.put("accessKeyId", ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_ACCESS_KEY"));
            s3ConfigMap.put("secretAccessKey", ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_SECRET_KEY"));
          }
          s3ConfigMap.put("assumeRoleArn", config.getS3AssumeRoleWithExternalId());
          s3ConfigMap.put("assumeRoleExternalId", config.getS3AssumeRoleExternalId());
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_CONFIG%%",
              jsonMapper.writeValueAsString(s3ConfigMap.build())
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_TYPE%%",
              "s3"
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_KEY%%",
              INPUT_SOURCE_OBJECTS_KEY
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
          INDEX_TASK_WITH_OVERRIDE,
          s3PropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
    }
  }

  void doTestS3WithAssumeRoleAndInvalidExternalIdShouldFail() throws Exception
  {
    if (config.getS3AssumeRoleExternalId() == null || config.getS3AssumeRoleWithExternalId() == null) {
      throw new SkipException("S3 Assume Role and external Id must be set for this test");
    }
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try {
      final Function<String, String> s3PropsTransform = spec -> {
        try {
          String inputSourceValue = jsonMapper.writeValueAsString(INPUT_SOURCE_OBJECTS_VALUE);
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
          ImmutableMap.Builder<String, Object> s3ConfigMap = ImmutableMap.builder();
          if (isSetS3OverrideCredentials()) {
            s3ConfigMap.put("accessKeyId", ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_ACCESS_KEY"));
            s3ConfigMap.put("secretAccessKey", ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_SECRET_KEY"));
          }
          s3ConfigMap.put("assumeRoleArn", config.getS3AssumeRoleWithExternalId());
          s3ConfigMap.put("assumeRoleExternalId", "RANDOM_INVALID_VALUE_" + UUID.randomUUID());
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_CONFIG%%",
              jsonMapper.writeValueAsString(s3ConfigMap.build())
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_TYPE%%",
              "s3"
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_KEY%%",
              INPUT_SOURCE_OBJECTS_KEY
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

      final String fullDatasourceName = indexDatasource + config.getExtraDatasourceNameSuffix();
      final String taskSpec = s3PropsTransform.apply(
          StringUtils.replace(
              getResourceAsString(INDEX_TASK_WITH_OVERRIDE),
              "%%DATASOURCE%%",
              fullDatasourceName
          )
      );
      final String taskID = indexer.submitTask(taskSpec);
      indexer.waitUntilTaskFails(taskID);
      TaskStatusPlus taskStatusPlus = indexer.getTaskStatus(taskID);
      // Index task is expected to fail as the external id is invalid
      Assert.assertEquals(taskStatusPlus.getStatusCode(), TaskState.FAILED);
      Assert.assertNotNull(taskStatusPlus.getErrorMsg());
      Assert.assertTrue(
          taskStatusPlus.getErrorMsg().contains("com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException"),
          "Expect task to fail with AWSSecurityTokenServiceException");
    }
    finally {
      // If the test pass, then there is no datasource to unload
      closeQuietly(unloader(indexDatasource + config.getExtraDatasourceNameSuffix()));
    }
  }

  void doTestS3WithValidAssumeRoleWithoutExternalIdShouldSucceed() throws Exception
  {
    if (config.getS3AssumeRoleWithoutExternalId() == null) {
      throw new SkipException("S3 Assume Role must be set for this test");
    }
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> s3PropsTransform = spec -> {
        try {
          String inputSourceValue = jsonMapper.writeValueAsString(INPUT_SOURCE_OBJECTS_VALUE);
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
          ImmutableMap.Builder<String, Object> s3ConfigMap = ImmutableMap.builder();
          if (isSetS3OverrideCredentials()) {
            s3ConfigMap.put("accessKeyId", ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_ACCESS_KEY"));
            s3ConfigMap.put("secretAccessKey", ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_SECRET_KEY"));
          }
          s3ConfigMap.put("assumeRoleArn", config.getS3AssumeRoleWithoutExternalId());
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_CONFIG%%",
              jsonMapper.writeValueAsString(s3ConfigMap.build())
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_TYPE%%",
              "s3"
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_KEY%%",
              INPUT_SOURCE_OBJECTS_KEY
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
          INDEX_TASK_WITH_OVERRIDE,
          s3PropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
    }
  }

  private void closeQuietly(Closeable closeable)
  {
    try {
      if (closeable != null) {
        closeable.close();
      }
    }
    catch (Exception var2) {
    }
  }
}
