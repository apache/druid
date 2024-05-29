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

package org.apache.druid.storage.s3.output;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.RuntimeInfo;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages the configuration for uploading files to S3 in chunks.
 * It tracks the number of chunks currently present on local disk and ensures that
 * it does not exceed a specified limit.
 */
public class S3UploadManager
{
  /**
   * The maximum chunk size based on injected values for {@link S3OutputConfig} and {@link S3ExportConfig} used for computing maximum number of chunks to save on disk at any given point in time.
   * It is initialized to 5 MiB which is the minimum chunk size possible, denoted by {@link S3OutputConfig#S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES}.
   */
  private long chunkSize = new HumanReadableBytes("5MiB").getBytes();

  private final AtomicInteger currentNumChunksOnDisk = new AtomicInteger(0);

  /**
   * The maximum number of chunks that can be saved to local disk concurrently.
   * This value is recalculated when the chunk size is updated in {@link #updateChunkSizeIfGreater(long)}.
   */
  private int maxConcurrentNumChunks = 100;

  private final ScheduledExecutorService uploadExecutor;

  private static final Logger log = new Logger(S3UploadManager.class);

  @Inject
  public S3UploadManager(ScheduledExecutorFactory scheduledExecutorFactory, RuntimeInfo runtimeInfo)
  {
    int poolSize = Math.max(4, runtimeInfo.getAvailableProcessors());
    this.uploadExecutor = scheduledExecutorFactory.create(
        poolSize,
        "UploadThreadPool-%d"
    );
  }

  /**
   * Increments the counter for the current number of chunks saved on disk.
   */
  public void incrementCurrentNumChunksOnDisk()
  {
    currentNumChunksOnDisk.incrementAndGet();
  }

  /**
   * Decrements the counter for the current number of chunks saved on disk.
   */
  public void decrementCurrentNumChunksOnDisk()
  {
    currentNumChunksOnDisk.decrementAndGet();
  }

  /**
   * Gets the current number of chunks saved to local disk.
   *
   * @return the current number of chunks saved to local disk.
   */
  public int getCurrentNumChunksOnDisk()
  {
    return currentNumChunksOnDisk.get();
  }

  /**
   * Gets the maximum number of concurrent chunks that can be saved to local disk.
   *
   * @return the maximum number of concurrent chunks that can be saved to local disk.
   */
  public int getMaxConcurrentNumChunks()
  {
    return maxConcurrentNumChunks;
  }

  /**
   * Updates the chunk size if the provided size is greater than the current chunk size.
   * Recomputes the maximum number of concurrent chunks that can be saved to local disk based on the new chunk size.
   *
   * @param chunkSize the new chunk size to be set if it is greater than the current chunk size.
   */
  public synchronized void updateChunkSizeIfGreater(long chunkSize)
  {
    if (this.chunkSize < chunkSize) {
      this.chunkSize = chunkSize;
      recomputeMaxConcurrentNumChunks();
    }
  }

  /**
   * Recomputes the maximum number of concurrent chunks that can be saved to local disk based on the current chunk size.
   * The maximum allowed chunk size is {@link S3OutputConfig#S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES} which is quite big,
   * so we restrict the maximum disk space used to the same, at any given point in time.
   */
  private void recomputeMaxConcurrentNumChunks()
  {
    maxConcurrentNumChunks = (int) (S3OutputConfig.S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES / chunkSize);
    log.info("Recomputed maxConcurrentNumChunks: %d", maxConcurrentNumChunks);
  }

  /**
   * Submit the runnable to the {@link #uploadExecutor}.
   * @param task
   */
  public void submitTask(Runnable task)
  {
    uploadExecutor.submit(task);
  }
}
