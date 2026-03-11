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

import org.apache.druid.data.input.s3.S3InputSourceConfig;
import org.apache.druid.data.input.s3.S3InputSourceDruidModule;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.minio.S3TestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This class defines methods to upload and delete the data files used by the tests, which will inherit this class.
 * The files are uploaded based on the values set for following environment variables.
 * "DRUID_CLOUD_BUCKET", "DRUID_CLOUD_PATH", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"
 * The test will fail if the above variables are not set.
 */
public abstract class AbstractS3InputSourceParallelIndexTest extends AbstractCloudInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(AbstractS3InputSourceParallelIndexTest.class);
  private static final String INDEX_TASK = "/indexer/wikipedia_cloud_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  protected final MinIOStorageResource minIOStorageResource = new MinIOStorageResource();
  private S3TestUtil s3;

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    cluster.addExtension(S3InputSourceDruidModule.class)
           .addResource(minIOStorageResource);
  }

  @BeforeAll
  public void uploadDataFilesToS3()
  {
    List<String> filesToUpload = new ArrayList<>();
    String localPath = "data/json/";
    for (String file : fileList()) {
      filesToUpload.add(localPath + file);
    }
    try {
      s3 = new S3TestUtil(
          minIOStorageResource.getS3Client(),
          getCloudBucket("s3"),
          getCloudPath("s3")
      );
      s3.createBucket();
      s3.uploadDataFilesToS3(filesToUpload);
    }
    catch (Exception e) {
      LOG.error(e, "Unable to upload files to s3");
      // Fail if exception
      Assertions.fail(e.getMessage());
    }
  }

  @AfterEach
  public void deleteSegmentsFromS3()
  {
    // Deleting folder created for storing segments (by druid) after test is completed
    s3.deleteFolderFromS3(dataSource);
  }

  @AfterAll
  public void deleteDataFilesFromS3()
  {
    // Deleting uploaded data files
    s3.deleteFilesFromS3(fileList());
  }

  /**
   * Variant of {@link #doTest} that injects an {@code endpointConfig} into the S3 input source spec.
   * Required when using per-input-source credentials: S3InputSource builds a new S3 client in that
   * case and must be told the MinIO endpoint explicitly, since it cannot inherit it from Druid's
   * global S3 config.
   */
  protected void doTestWithEndpointConfig(
      Pair<String, List<?>> inputSource,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair,
      S3InputSourceConfig s3InputSourceConfig,
      String endpointUrl
  ) throws Exception
  {
    final String indexDatasource = dataSource;
    try (final Closeable ignored = unloader(indexDatasource)) {
      final String endpointConfigJson = jsonMapper.writeValueAsString(
          Map.of("url", endpointUrl, "signingRegion", "us-east-1")
      );
      // Path-style access is required for MinIO running at a local IP address
      final String clientConfigJson = jsonMapper.writeValueAsString(
          Map.of("enablePathStyleAccess", true)
      );
      final Function<String, String> transform = spec -> {
        try {
          String inputSourceValue = jsonMapper.writeValueAsString(inputSource.rhs);
          inputSourceValue = setInputSourceInPath("s3", inputSourceValue);
          inputSourceValue = StringUtils.replace(inputSourceValue, "%%BUCKET%%", getCloudBucket("s3"));
          inputSourceValue = StringUtils.replace(inputSourceValue, "%%PATH%%", getCloudPath("s3"));
          spec = StringUtils.replace(spec, "%%INPUT_FORMAT_TYPE%%", InputFormatDetails.JSON.getInputFormatType());
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(spec, "%%INPUT_SOURCE_TYPE%%", "s3");
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTIES%%",
              jsonMapper.writeValueAsString(s3InputSourceConfig)
          );
          spec = StringUtils.replace(spec, "%%INPUT_SOURCE_PROPERTY_KEY%%", inputSource.lhs);
          spec = StringUtils.replace(spec, "%%INPUT_SOURCE_PROPERTY_VALUE%%", inputSourceValue);
          // Inject endpointConfig and clientConfig so the custom S3 client points at MinIO with path-style access
          spec = StringUtils.replace(
              spec,
              "\"type\": \"s3\",",
              "\"type\": \"s3\", \"endpointConfig\": "
              + endpointConfigJson
              + ", \"clientConfig\": "
              + clientConfigJson
              + ","
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
          transform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          segmentAvailabilityConfirmationPair
      );
    }
  }
}
