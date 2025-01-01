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

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Stopwatch;
import com.google.common.io.CountingOutputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A retryable output stream for s3. How it works is:
 * <p>
 * <ol>
 * <li>When new data is written, it first creates a chunk in local disk.</li>
 * <li>New data is written to the local chunk until it is full.</li>
 * <li>When the chunk is full, it uploads the chunk to s3 using the multipart upload API.
 * Since this happens synchronously, {@link #write(byte[], int, int)} can be blocked until the upload is done.
 * The upload can be retried when it fails with transient errors.</li>
 * <li>Once the upload succeeds, it creates a new chunk and continue.</li>
 * <li>When the stream is closed, it uploads the last chunk and finalize the multipart upload.
 * {@link #close()} can be blocked until upload is done.</li>
 *   </ol>
 * For compression format support, this output stream supports compression formats if they are <i>concatenatable</i>,
 * such as ZIP or GZIP.
 * <p>
 * This class is not thread-safe.
 * <p>
 */
public class RetryableS3OutputStream extends OutputStream
{
  // Metric related constants.
  private static final String METRIC_PREFIX = "s3/upload/total/";
  private static final String METRIC_TOTAL_UPLOAD_TIME = METRIC_PREFIX + "time";
  private static final String METRIC_TOTAL_UPLOAD_BYTES = METRIC_PREFIX + "bytes";

  private static final Logger LOG = new Logger(RetryableS3OutputStream.class);

  private final S3OutputConfig config;
  private final ServerSideEncryptingAmazonS3 s3;
  private final String s3Key;
  private final String uploadId;
  private final File chunkStorePath;
  private final long chunkSize;

  private final byte[] singularBuffer = new byte[1];

  // metric
  private final Stopwatch pushStopwatch;

  private Chunk currentChunk;
  private int nextChunkId = 1; // multipart upload requires partNumber to be in the range between 1 and 10000

  /**
   * A flag indicating whether there was an upload error.
   * This flag is tested in {@link #close()} to determine whether it needs to upload the current chunk or not.
   */
  private boolean error;
  private boolean closed;

  /**
   * Helper class for calculating maximum number of simultaneous chunks allowed on local disk.
   */
  private final S3UploadManager uploadManager;

  /**
   * A list of futures to allow us to wait for completion of all uploadPart() calls
   * before hitting {@link ServerSideEncryptingAmazonS3#completeMultipartUpload(CompleteMultipartUploadRequest)}.
   */
  private final List<Future<UploadPartResult>> futures = new ArrayList<>();

  public RetryableS3OutputStream(
      S3OutputConfig config,
      ServerSideEncryptingAmazonS3 s3,
      String s3Key,
      S3UploadManager uploadManager
  ) throws IOException
  {
    this.config = config;
    this.s3 = s3;
    this.s3Key = s3Key;
    this.uploadManager = uploadManager;

    final InitiateMultipartUploadResult result;
    try {
      result = S3Utils.retryS3Operation(() -> s3.initiateMultipartUpload(
          new InitiateMultipartUploadRequest(config.getBucket(), s3Key)
      ), config.getMaxRetry());
    }
    catch (Exception e) {
      throw new IOException("Unable to start multipart upload", e);
    }
    this.uploadId = result.getUploadId();
    this.chunkStorePath = new File(config.getTempDir(), uploadId + UUID.randomUUID());
    FileUtils.mkdirp(this.chunkStorePath);
    this.chunkSize = config.getChunkSize();
    this.pushStopwatch = Stopwatch.createStarted();
    this.currentChunk = new Chunk(nextChunkId, new File(chunkStorePath, String.valueOf(nextChunkId++)));
  }


  @Override
  public void write(int b) throws IOException
  {
    singularBuffer[0] = (byte) b;
    write(singularBuffer, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    if (b == null) {
      error = true;
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
               ((off + len) > b.length) || ((off + len) < 0)) {
      error = true;
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    try {
      int offsetToWrite = off;
      int remainingBytesToWrite = len;

      while (remainingBytesToWrite > 0) {
        final int writtenBytes = writeToCurrentChunk(b, offsetToWrite, remainingBytesToWrite);
        if (currentChunk.length() >= chunkSize) {
          pushCurrentChunk();
          currentChunk = new Chunk(nextChunkId, new File(chunkStorePath, String.valueOf(nextChunkId++)));
        }

        offsetToWrite += writtenBytes;
        remainingBytesToWrite -= writtenBytes;
      }
    }
    catch (RuntimeException | IOException e) {
      error = true;
      throw e;
    }
  }

  private int writeToCurrentChunk(byte[] b, int off, int len) throws IOException
  {
    final int lenToWrite = Math.min(len, Math.toIntExact(chunkSize - currentChunk.length()));
    currentChunk.outputStream.write(b, off, lenToWrite);
    return lenToWrite;
  }

  private void pushCurrentChunk() throws IOException
  {
    currentChunk.close();
    final Chunk chunk = currentChunk;
    if (chunk.length() > 0) {
      futures.add(
          uploadManager.queueChunkForUpload(s3, s3Key, chunk.id, chunk.file, uploadId, config)
      );
    }
  }

  @Override
  public void close() throws IOException
  {
    if (closed) {
      return;
    }
    closed = true;
    Closer closer = Closer.create();

    // Closeables are closed in LIFO order
    closer.register(() -> {
      org.apache.commons.io.FileUtils.forceDelete(chunkStorePath);

      final long totalBytesUploaded = (currentChunk.id - 1) * chunkSize + currentChunk.length();
      final long totalUploadTimeMillis = pushStopwatch.elapsed(TimeUnit.MILLISECONDS);
      LOG.debug(
          "Pushed total [%d] parts containing [%d] bytes in [%d]ms for s3Key[%s], uploadId[%s].",
          futures.size(),
          totalBytesUploaded,
          totalUploadTimeMillis,
          s3Key,
          uploadId
      );

      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder().setDimension("uploadId", uploadId);
      uploadManager.emitMetric(builder.setMetric(METRIC_TOTAL_UPLOAD_TIME, totalUploadTimeMillis));
      uploadManager.emitMetric(builder.setMetric(METRIC_TOTAL_UPLOAD_BYTES, totalBytesUploaded));
    });

    try (Closer ignored = closer) {
      if (!error) {
        pushCurrentChunk();
        completeMultipartUpload();
      }
    }
  }

  private void completeMultipartUpload()
  {
    final List<PartETag> pushResults = new ArrayList<>();
    for (Future<UploadPartResult> future : futures) {
      if (error) {
        future.cancel(true);
      }
      try {
        UploadPartResult result = future.get(1, TimeUnit.HOURS);
        pushResults.add(result.getPartETag());
      }
      catch (Exception e) {
        error = true;
        LOG.error(e, "Error in uploading part for upload ID [%s]", uploadId);
      }
    }

    try {
      boolean isAllPushSucceeded = !error && !pushResults.isEmpty() && futures.size() == pushResults.size();
      if (isAllPushSucceeded) {
        RetryUtils.retry(
            () -> s3.completeMultipartUpload(
                new CompleteMultipartUploadRequest(config.getBucket(), s3Key, uploadId, pushResults)
            ),
            S3Utils.S3RETRY,
            config.getMaxRetry()
        );
      } else {
        RetryUtils.retry(
            () -> {
              s3.cancelMultiPartUpload(new AbortMultipartUploadRequest(config.getBucket(), s3Key, uploadId));
              return null;
            },
            S3Utils.S3RETRY,
            config.getMaxRetry()
        );
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class Chunk implements Closeable
  {
    private final int id;
    private final File file;
    private final CountingOutputStream outputStream;
    private boolean closed;

    private Chunk(int id, File file) throws FileNotFoundException
    {
      this.id = id;
      this.file = file;
      this.outputStream = new CountingOutputStream(new FastBufferedOutputStream(new FileOutputStream(file)));
    }

    private long length()
    {
      return outputStream.getCount();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Chunk chunk = (Chunk) o;
      return id == chunk.id;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(id);
    }

    @Override
    public void close() throws IOException
    {
      if (closed) {
        return;
      }
      closed = true;
      outputStream.close();
    }

    @Override
    public String toString()
    {
      return "Chunk{" +
             "id=" + id +
             ", file=" + file.getAbsolutePath() +
             ", size=" + length() +
             '}';
    }
  }
}
