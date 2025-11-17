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

package org.apache.druid.testing.embedded.msq;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;

import java.io.File;

/**
 * Resource that configures MSQ to use a bucket from {@link MinIOStorageResource} for durable intermediate data storage.
 */
public class MinIODurableStorageResource implements EmbeddedResource
{
  private final MinIOStorageResource storageResource;

  public MinIODurableStorageResource(MinIOStorageResource storageResource)
  {
    this.storageResource = storageResource;
  }

  @Override
  public void start()
  {
    // Nothing to do.
  }

  @Override
  public void stop()
  {
    // Nothing to do.
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    final File intermediateTempDir = cluster.getTestFolder().getOrCreateFolder("msq-shuffle-storage-tmp");
    cluster.addCommonProperty("druid.msq.intermediate.storage.enable", "true");
    cluster.addCommonProperty("druid.msq.intermediate.storage.type", "s3");
    cluster.addCommonProperty("druid.msq.intermediate.storage.tempDir", intermediateTempDir.getAbsolutePath());
    cluster.addCommonProperty("druid.msq.intermediate.storage.bucket", storageResource.getBucket());
    cluster.addCommonProperty("druid.msq.intermediate.storage.prefix", getBaseKey());

    // Set tmpStorageBytesPerTask to 3 GB. This controls when stage-internal channels (such as ones used internally
    // by SuperSorter) "spill over" from local disk to durable storage. Cannot set this much lower currently, due to
    // validations in WorkerStorageParameters. Ideally, we'd like to set this low enough such that embedded tests are
    // actually using minio/S3 for stage-internal storage. At this level, they won't be using it, but at least stages
    // will go through the motions of setting up composing channels internally. There is still value in exercising
    // that code path.
    //
    // Note: this below property only controls the behavior of stage-internal channels. With the above intermediate
    // storage configs, stage *output* will *always* go to durable storage.
    cluster.addCommonProperty("druid.indexer.task.tmpStorageBytesPerTask", "3000000000");
  }

  /**
   * Returns the value of {@code druid.msq.intermediate.storage.prefix}.
   */
  public String getBaseKey()
  {
    return StringUtils.format("%s/%s", storageResource.getBaseKey(), "msq-intermediate");
  }
}
