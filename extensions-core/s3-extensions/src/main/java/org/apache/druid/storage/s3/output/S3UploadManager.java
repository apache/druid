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

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.RuntimeInfo;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages uploading files to S3 in chunks, while ensuring that the
 * number of chunks currently present on local disk does not exceed a specific limit.
 */
@ManageLifecycle
public class S3UploadManager
{
  // Metric related constants.
  private static final String METRIC_PREFIX = "s3/upload/part/";
  private static final String METRIC_PART_QUEUED_TIME = METRIC_PREFIX + "queuedTime";
  private static final String METRIC_QUEUE_SIZE = METRIC_PREFIX + "queueSize";
  private static final String METRIC_PART_UPLOAD_TIME = METRIC_PREFIX + "time";

  private final ExecutorService uploadExecutor;
  private final ServiceEmitter emitter;

  private static final Logger log = new Logger(S3UploadManager.class);

  // For metrics regarding uploadExecutor.
  private final AtomicInteger executorQueueSize = new AtomicInteger(0);

  @Inject
  public S3UploadManager(
      S3OutputConfig s3OutputConfig,
      S3ExportConfig s3ExportConfig,
      RuntimeInfo runtimeInfo,
      ServiceEmitter emitter
  )
  {
    int poolSize = Math.max(4, runtimeInfo.getAvailableProcessors());
    int maxNumChunksOnDisk = computeMaxNumChunksOnDisk(s3OutputConfig, s3ExportConfig);
    this.uploadExecutor = createExecutorService(poolSize, maxNumChunksOnDisk);
    log.info("Initialized executor service for S3 multipart upload with pool size [%d] and work queue capacity [%d]",
             poolSize, maxNumChunksOnDisk);
    this.emitter = emitter;
  }

  /**
   * Computes the maximum number of S3 upload chunks that can be kept on disk using the
   * maximum chunk size specified in {@link S3OutputConfig} and {@link S3ExportConfig}.
   */
  public static int computeMaxNumChunksOnDisk(S3OutputConfig s3OutputConfig, S3ExportConfig s3ExportConfig)
  {
    long maxChunkSize = S3OutputConfig.S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES;
    if (s3OutputConfig != null && s3OutputConfig.getChunkSize() != null) {
      maxChunkSize = Math.max(maxChunkSize, s3OutputConfig.getChunkSize());
    }
    if (s3ExportConfig != null && s3ExportConfig.getChunkSize() != null) {
      maxChunkSize = Math.max(maxChunkSize, s3ExportConfig.getChunkSize().getBytes());
    }

    return (int) (S3OutputConfig.S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES / maxChunkSize);
  }

  /**
   * Queues a chunk of a file for upload to S3 as part of a multipart upload.
   */
  public Future<UploadPartResult> queueChunkForUpload(
      ServerSideEncryptingAmazonS3 s3Client,
      String key,
      int chunkNumber,
      File chunkFile,
      String uploadId,
      S3OutputConfig config
  )
  {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    executorQueueSize.incrementAndGet();
    return uploadExecutor.submit(() -> {
      final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
      emitMetric(metricBuilder.setMetric(METRIC_QUEUE_SIZE, executorQueueSize.decrementAndGet()));
      metricBuilder.setDimension("uploadId", uploadId).setDimension("partNumber", chunkNumber);
      emitMetric(metricBuilder.setMetric(METRIC_PART_QUEUED_TIME, stopwatch.millisElapsed()));
      stopwatch.restart();

      return RetryUtils.retry(
          () -> {
            log.debug("Uploading chunk[%d] for uploadId[%s].", chunkNumber, uploadId);
            UploadPartResult uploadPartResult = uploadPartIfPossible(
                s3Client,
                uploadId,
                config.getBucket(),
                key,
                chunkNumber,
                chunkFile
            );
            if (!chunkFile.delete()) {
              log.warn("Failed to delete chunk [%s]", chunkFile.getAbsolutePath());
            }
            emitMetric(metricBuilder.setMetric(METRIC_PART_UPLOAD_TIME, stopwatch.millisElapsed()));
            return uploadPartResult;
          },
          S3Utils.S3RETRY,
          config.getMaxRetry()
      );
    });
  }

  @VisibleForTesting
  UploadPartResult uploadPartIfPossible(
      ServerSideEncryptingAmazonS3 s3Client,
      String uploadId,
      String bucket,
      String key,
      int chunkNumber,
      File chunkFile
  )
  {
    final UploadPartRequest uploadPartRequest = new UploadPartRequest()
        .withUploadId(uploadId)
        .withBucketName(bucket)
        .withKey(key)
        .withFile(chunkFile)
        .withPartNumber(chunkNumber)
        .withPartSize(chunkFile.length());

    if (log.isDebugEnabled()) {
      log.debug("Pushing chunk[%s] to bucket[%s] and key[%s].", chunkNumber, bucket, key);
    }
    return s3Client.uploadPart(uploadPartRequest);
  }

  private ExecutorService createExecutorService(int poolSize, int maxNumConcurrentChunks)
  {
    return Execs.newBlockingThreaded("S3UploadThreadPool-%d", poolSize, maxNumConcurrentChunks);
  }

  @LifecycleStart
  public void start()
  {
    // No state startup required
  }

  @LifecycleStop
  public void stop()
  {
    uploadExecutor.shutdown();
  }

  protected void emitMetric(ServiceMetricEvent.Builder builder)
  {
    emitter.emit(builder);
  }
}
