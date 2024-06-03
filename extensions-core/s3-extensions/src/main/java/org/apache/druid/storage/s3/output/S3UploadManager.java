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
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.RuntimeInfo;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * This class manages uploading files to S3 in chunks, while ensuring that the
 * number of chunks currently present on local disk does not exceed a specific limit.
 */
@ManageLifecycle
public class S3UploadManager
{
  private final ExecutorService uploadExecutor;

  private static final Logger log = new Logger(S3UploadManager.class);

  @Inject
  public S3UploadManager(S3OutputConfig s3OutputConfig, S3ExportConfig s3ExportConfig, RuntimeInfo runtimeInfo)
  {
    int poolSize = Math.max(4, runtimeInfo.getAvailableProcessors());
    int maxNumConcurrentChunks = computeMaxNumConcurrentChunks(s3OutputConfig, s3ExportConfig);
    this.uploadExecutor = Execs.newBlockingThreaded("UploadThreadPool-%d", poolSize, maxNumConcurrentChunks);
    log.info("Initialized executor service for S3 multipart upload with pool size [%d] and work queue capacity [%d]",
             poolSize, maxNumConcurrentChunks);
  }

  /**
   * Computes the maximum number of concurrent chunks for an S3 multipart upload.
   * We want to determine the maximum number of concurrent chunks on disk based on the maximum value of chunkSize
   * between the 2 configs: S3OutputConfig and S3ExportConfig.
   *
   * @param s3OutputConfig  The S3 output configuration, which may specify a custom chunk size.
   * @param s3ExportConfig  The S3 export configuration, which may also specify a custom chunk size.
   * @return The maximum number of concurrent chunks.
   */
  private int computeMaxNumConcurrentChunks(S3OutputConfig s3OutputConfig, S3ExportConfig s3ExportConfig)
  {
    long chunkSize = S3OutputConfig.S3_MULTIPART_UPLOAD_MIN_PART_SIZE_BYTES;
    if (s3OutputConfig != null && s3OutputConfig.getChunkSize() != null) {
      chunkSize = Math.max(chunkSize, s3OutputConfig.getChunkSize());
    }
    if (s3ExportConfig != null && s3ExportConfig.getChunkSize() != null) {
      chunkSize = Math.max(chunkSize, s3ExportConfig.getChunkSize().getBytes());
    }

    return (int) (S3OutputConfig.S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES / chunkSize);
  }

  /**
   * Queues a chunk of a file for upload to S3 as part of a multipart upload.
   */
  public Future<UploadPartResult> queueChunkForUpload(ServerSideEncryptingAmazonS3 s3Client, String key, int chunkNumber, File chunkFile, String uploadId, S3OutputConfig config)
  {
    return uploadExecutor.submit(() -> RetryUtils.retry(
        () -> {
          log.info("Uploading chunk [%d] for uploadId [%s].", chunkNumber, uploadId);
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
          return uploadPartResult;
        },
        S3Utils.S3RETRY,
        config.getMaxRetry()
    ));
  }

  private UploadPartResult uploadPartIfPossible(
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
      log.debug("Pushing chunk [%s] to bucket[%s] and key[%s].", chunkNumber, bucket, key);
    }
    return s3Client.uploadPart(uploadPartRequest);
  }

  @LifecycleStart
  public void start()
  {
    // No state startup required
  }

  @LifecycleStop
  public void stop()
  {
    log.debug("Stopping S3UploadManager");
    uploadExecutor.shutdown();
    log.debug("Stopped S3UploadManager");
  }

}
