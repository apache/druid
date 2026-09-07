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

package org.apache.druid.testing.embedded.oss;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexer.AbstractCloudInputSourceParallelIndexTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Abstract base for embedded OSS-to-OSS parallel index tests. These tests require real
 * Alibaba Cloud OSS credentials and are skipped when the {@link java.util.Properties} are not set.
 *
 * <p>Set the following runtime properties to enable the tests:
 * <ul>
 *   <li>{@code druid.testing.oss.access} – Aliyun access key ID</li>
 *   <li>{@code druid.testing.oss.secret} – Aliyun secret access key</li>
 *   <li>{@code druid.testing.oss.endpoint} – OSS endpoint (e.g. {@code oss-cn-hangzhou.aliyuncs.com})</li>
 *   <li>{@code druid.testing.oss.bucket} – Bucket for deep storage and test data</li>
 *   <li>{@code druid.testing.oss.path} – Optional key prefix for test input data (default: {@code path})</li>
 * </ul>
 */
public abstract class AbstractOssInputSourceParallelIndexTest extends AbstractCloudInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(AbstractOssInputSourceParallelIndexTest.class);

  private final OssStorageResource ossStorageResource = new OssStorageResource();
  private OssTestUtil oss;

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    cluster.addResource(ossStorageResource);
  }

  @Override
  public String getCloudBucket(String inputSourceType)
  {
    return ossStorageResource.getBucket();
  }

  @Override
  public String getCloudPath(String inputSourceType)
  {
    return ossStorageResource.getPath();
  }

  @BeforeAll
  public void setupOss()
  {
    assumeTrue(
        ossStorageResource.isConfigured(),
        "Aliyun OSS credentials not configured. Set druid.testing.oss.access, druid.testing.oss.secret, "
        + "druid.testing.oss.endpoint, and druid.testing.oss.bucket to run these tests."
    );
    final List<String> filesToUpload = new ArrayList<>();
    final String localPath = "data/json/";
    for (String file : fileList()) {
      filesToUpload.add(localPath + file);
    }
    try {
      oss = new OssTestUtil(
          ossStorageResource.buildOssClient(),
          ossStorageResource.getBucket(),
          ossStorageResource.getPath()
      );
      oss.uploadDataFilesToOss(filesToUpload);
    }
    catch (Exception e) {
      LOG.error(e, "Unable to upload files to OSS");
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  public void deleteSegmentsFromOss()
  {
    if (oss != null) {
      oss.deleteFolderFromOss(dataSource);
    }
  }

  @AfterAll
  public void deleteDataFilesFromOss()
  {
    if (oss != null) {
      oss.deleteFilesFromOss(fileList());
    }
  }
}
