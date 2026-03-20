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

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.aliyun.OssInputSourceDruidModule;
import org.apache.druid.storage.aliyun.OssStorageDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;

import javax.annotation.Nullable;

/**
 * Configures the embedded cluster to use Alibaba Cloud OSS for deep storage of segments
 * and task logs. Credentials and endpoint are read from {@link java.util.Properties}:
 * <ul>
 *   <li>{@code druid.testing.oss.access} – Aliyun access key ID</li>
 *   <li>{@code druid.testing.oss.secret} – Aliyun secret access key</li>
 *   <li>{@code druid.testing.oss.endpoint} – OSS endpoint (e.g. {@code oss-cn-hangzhou.aliyuncs.com})</li>
 *   <li>{@code druid.testing.oss.bucket} – Bucket for deep storage and test data</li>
 *   <li>{@code druid.testing.oss.path} – Optional key prefix for test input data (default: {@code path})</li>
 * </ul>
 * Tests using this resource skip themselves when the runtime properties are not set.
 */
public class OssStorageResource implements EmbeddedResource
{
  public static final String ACCESS_KEY_PROPERTY = "druid.testing.oss.access";
  public static final String SECRET_KEY_PROPERTY = "druid.testing.oss.secret";
  public static final String ENDPOINT_PROPERTY = "druid.testing.oss.endpoint";
  public static final String BUCKET_PROPERTY = "druid.testing.oss.bucket";
  public static final String PATH_PROPERTY = "druid.testing.oss.path";

  @Override
  public void start()
  {
    // No container to start; configuration comes from system properties.
  }

  @Override
  public void stop()
  {
    // Nothing to tear down.
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    if (!isConfigured()) {
      // Credentials not available; tests will be skipped via assumeTrue() in @BeforeAll.
      // Do not configure OSS on the cluster to avoid startup failures from missing properties.
      return;
    }

    cluster.addExtension(OssStorageDruidModule.class);
    cluster.addExtension(OssInputSourceDruidModule.class);

    // Deep storage
    cluster.addCommonProperty("druid.storage.type", "oss");
    cluster.addCommonProperty("druid.storage.oss.bucket", getBucket());
    cluster.addCommonProperty("druid.storage.oss.prefix", "druid/segments");

    // Indexer task logs
    cluster.addCommonProperty("druid.indexer.logs.type", "oss");
    cluster.addCommonProperty("druid.indexer.logs.oss.bucket", getBucket());
    cluster.addCommonProperty("druid.indexer.logs.oss.prefix", "druid/indexing-logs");

    // OSS client credentials
    cluster.addCommonProperty("druid.oss.endpoint", getEndpoint());
    cluster.addCommonProperty("druid.oss.accessKey", getAccessKey());
    cluster.addCommonProperty("druid.oss.secretKey", getSecretKey());
  }

  public OSS buildOssClient()
  {
    return new OSSClientBuilder().build(getEndpoint(), getAccessKey(), getSecretKey());
  }

  @Nullable
  public static String getProperty(String name)
  {
    return System.getProperty(name);
  }

  public boolean isConfigured()
  {
    return getAccessKey() != null
           && getSecretKey() != null
           && getEndpoint() != null
           && getBucket() != null;
  }

  public String getAccessKey()
  {
    return getProperty(ACCESS_KEY_PROPERTY);
  }

  public String getSecretKey()
  {
    return getProperty(SECRET_KEY_PROPERTY);
  }

  public String getEndpoint()
  {
    return getProperty(ENDPOINT_PROPERTY);
  }

  public String getBucket()
  {
    return getProperty(BUCKET_PROPERTY);
  }

  public String getPath()
  {
    return Configs.valueOrDefault(getProperty(PATH_PROPERTY), "path");
  }
}
