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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.UUID;
import java.util.function.Function;

/**
 * IMPORTANT:
 * To run this test, you must:
 * 1) Set the bucket and path for your data. This can be done by setting -Ddruid.test.config.cloudBucket and
 *    -Ddruid.test.config.cloudPath or setting "cloud_bucket" and "cloud_path" in the config file.
 * 2) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
 *    located in integration-tests/src/test/resources/data/batch_index/json to your S3 at the location set in step 1.
 * 3) Provide -Doverride.config.path=<PATH_TO_FILE> with s3 credentials/configs set. See
 *    integration-tests/docker/environment-configs/override-examples/s3 for env vars to provide.
 *    Note that druid_s3_accessKey and druid_s3_secretKey should be unset or set to credentials that does not have
 *    access to the bucket and path specified in #1. The credentials that does have access to the bucket and path
 *    specified in #1 should be set to the env variable OVERRIDE_S3_ACCESS_KEY and OVERRIDE_S3_SECRET_KEY
 */
@Test(groups = TestNGGroup.S3_INGESTION)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITS3OverrideCredentialsIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK_WITH_OVERRIDE = "/indexer/wikipedia_override_credentials_index_task.json";
  private static final String INDEX_TASK_WITHOUT_OVERRIDE = "/indexer/wikipedia_cloud_simple_index_task.json";
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

  @Test
  public void testS3WithValidOverrideCredentialsIndexDataShouldSucceed() throws Exception
  {
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

          spec = StringUtils.replace(
              spec,
              "%%ACCESS_KEY_PROPERTY_VALUE%%",
              jsonMapper.writeValueAsString(
                  ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_ACCESS_KEY")
              )
          );
          spec = StringUtils.replace(
              spec,
              "%%SECRET_KEY_PROPERTY_VALUE%%",
              jsonMapper.writeValueAsString(
                  ImmutableMap.of("type", "environment", "variable", "OVERRIDE_S3_SECRET_KEY")
              )
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
          true
      );
    }
  }

  @Test
  public void testS3WithoutOverrideCredentialsIndexDataShouldFailed() throws Exception
  {
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
              getResourceAsString(INDEX_TASK_WITHOUT_OVERRIDE),
              "%%DATASOURCE%%",
              fullDatasourceName
          )
      );
      final String taskID = indexer.submitTask(taskSpec);
      indexer.waitUntilTaskFails(taskID);
      TaskStatusPlus taskStatusPlus = indexer.getTaskStatus(taskID);
      // Index task is expected to fail as the default S3 Credentials in Druid's config (druid.s3.accessKey and
      // druid.s3.secretKey should not have access to the bucket and path for our data. (Refer to the setup instruction
      // at the top of this test class.
      Assert.assertEquals(taskStatusPlus.getStatusCode(), TaskState.FAILED);
      Assert.assertNotNull(taskStatusPlus.getErrorMsg());
      Assert.assertTrue(
          taskStatusPlus.getErrorMsg().contains("com.amazonaws.services.s3.model.AmazonS3Exception"),
          "Expect task to fail with AmazonS3Exception");
    }
    finally {
      // If the test pass, then there is no datasource to unload
      closeQuietly(unloader(indexDatasource + config.getExtraDatasourceNameSuffix()));
    }
  }

  @Test
  public void testS3WithInvalidOverrideCredentialsIndexDataShouldFailed() throws Exception
  {
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

          spec = StringUtils.replace(
              spec,
              "%%ACCESS_KEY_PROPERTY_VALUE%%",
              jsonMapper.writeValueAsString(
                  ImmutableMap.of("type", "environment", "variable", "NON_EXISTENT_INVALID_ENV_VAR")
              )
          );
          spec = StringUtils.replace(
              spec,
              "%%SECRET_KEY_PROPERTY_VALUE%%",
              jsonMapper.writeValueAsString(
                  ImmutableMap.of("type", "environment", "variable", "NON_EXISTENT_INVALID_ENV_VAR")
              )
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
      // Index task is expected to fail as the overrided s3 access key and s3 secret key cannot be null
      Assert.assertEquals(taskStatusPlus.getStatusCode(), TaskState.FAILED);
      Assert.assertNotNull(taskStatusPlus.getErrorMsg());
      Assert.assertTrue(
          taskStatusPlus.getErrorMsg().contains("IllegalArgumentException: Access key cannot be null"),
          "Expect task to fail with IllegalArgumentException: Access key cannot be null");
    }
    finally {
      // If the test pass, then there is no datasource to unload
      closeQuietly(unloader(indexDatasource + config.getExtraDatasourceNameSuffix()));
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
