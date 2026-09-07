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

package org.apache.druid.testing.embedded.gcs;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;

/**
 * Configures the embedded cluster to use Google Cloud Storage for deep storage
 * of segments and task logs.
 */
public class GoogleCloudStorageResource extends TestcontainerResource<FakeGcsServerContainer>
{
  private static final String BUCKET = "druid-deep-storage";
  private static final String PATH_PREFIX = "druid/segments";

  @Override
  protected FakeGcsServerContainer createContainer()
  {
    return new FakeGcsServerContainer();
  }

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    super.beforeStart(cluster);
    cluster.addExtension(GoogleStorageTestModule.class);

    // Configure storage bucket and base key
    cluster.addCommonProperty("druid.storage.type", "google");
    cluster.addCommonProperty("druid.google.bucket", getBucket());
    cluster.addCommonProperty("druid.google.prefix", getPathPrefix());

    // Configure indexer logs
    cluster.addCommonProperty("druid.indexer.logs.type", "google");
    cluster.addCommonProperty("druid.indexer.logs.bucket", getBucket());
    cluster.addCommonProperty("druid.indexer.logs.prefix", "druid/indexing-logs");
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    final String connectionUrl = getUrl();
    cluster.addCommonProperty("druid.google.storageUrl", connectionUrl);

    try (Storage storage = GoogleStorageTestModule.createStorageForTests(connectionUrl)) {
      storage.create(BucketInfo.of(getBucket()));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getUrl()
  {
    ensureRunning();
    return getContainer().url();
  }

  public String getBucket()
  {
    return BUCKET;
  }

  public String getPathPrefix()
  {
    return PATH_PREFIX;
  }
}
