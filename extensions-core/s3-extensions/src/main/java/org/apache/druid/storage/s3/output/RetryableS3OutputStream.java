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

import com.google.common.base.Stopwatch;
import com.google.common.io.CountingOutputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import javax.annotation.Nullable;
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
 * <li>When the chunk is full, a multipart upload is started if one is not already in progress, and the chunk is
 * queued for upload as a part. {@link #write(byte[], int, int)} can be blocked while the upload queue is saturated.
 * The upload can be retried when it fails with transient errors.</li>
 * <li>Once the chunk is queued, it creates a new chunk and continue.</li>
 * <li>When the stream is closed, what happens depends on whether any part was uploaded. A stream that never filled
 * a chunk is uploaded as a single object, costing one request instead of the three a multipart upload needs;
 * otherwise the last chunk is uploaded and the multipart upload is finalized. {@link #close()} can be blocked
 * until upload is done.</li>
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
  private final File chunkStorePath;
  private final long chunkSize;

  /**
   * Multipart upload ID, or null while the stream still fits in {@link #currentChunk}. A multipart upload costs a
   * create and a complete request on top of the part uploads themselves, so it is only started once a chunk actually
   * needs pushing; a stream that never fills a chunk is uploaded by {@link #close()} as a single putObject.
   *
   * @see #initiateMultipartUploadIfNeeded()
   */
  @Nullable
  private String uploadId;

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
   * before hitting {@link ServerSideEncryptingAmazonS3#completeMultipartUpload}.
   */
  private final List<Future<UploadPartResponse>> futures = new ArrayList<>();

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

    this.chunkStorePath = new File(config.getTempDir(), UUID.randomUUID().toString());
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
      initiateMultipartUploadIfNeeded();
      futures.add(
          uploadManager.queueChunkForUpload(s3, s3Key, chunk.id, chunk.file, uploadId, config)
      );
    }
  }

  /**
   * Starts the multipart upload the first time a chunk actually needs pushing. Called only from
   * {@link #pushCurrentChunk()}, so a stream that never fills a chunk issues no create request at all.
   */
  private void initiateMultipartUploadIfNeeded() throws IOException
  {
    if (uploadId != null) {
      return;
    }
    try {
      final CreateMultipartUploadRequest.Builder requestBuilder = CreateMultipartUploadRequest.builder()
          .bucket(config.getBucket())
          .key(s3Key);
      final CreateMultipartUploadResponse result = S3Utils.retryS3Operation(
          () -> s3.createMultipartUpload(requestBuilder),
          config.getMaxRetry()
      );
      uploadId = result.uploadId();
    }
    catch (Exception e) {
      throw new IOException("Unable to start multipart upload", e);
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

      final ServiceMetricEvent.Builder builder =
          new ServiceMetricEvent.Builder().setDimension("uploadId", uploadId == null ? "none" : uploadId);
      uploadManager.emitMetric(builder.setMetric(METRIC_TOTAL_UPLOAD_TIME, totalUploadTimeMillis));
      uploadManager.emitMetric(builder.setMetric(METRIC_TOTAL_UPLOAD_BYTES, totalBytesUploaded));
    });

    try (Closer ignored = closer) {
      if (!error) {
        if (uploadId == null) {
          // Everything written fits in the first chunk, so a single putObject does the job that a create, an upload
          // part and a complete would otherwise take.
          putCurrentChunkAsWholeObject();
        } else {
          pushCurrentChunk();
          completeMultipartUpload();
        }
      }
    }
  }

  /**
   * Uploads {@link #currentChunk} as a complete object. Only valid while {@link #uploadId} is null, i.e. when no part
   * has ever been pushed and the chunk therefore holds the entire stream.
   */
  private void putCurrentChunkAsWholeObject() throws IOException
  {
    currentChunk.close();
    if (currentChunk.length() == 0) {
      // Nothing was written, so there is no object to create.
      return;
    }
    try {
      S3Utils.retryS3Operation(
          () -> s3.putObject(config.getBucket(), s3Key, currentChunk.file),
          config.getMaxRetry()
      );
    }
    catch (Exception e) {
      throw new IOException(StringUtils.format("Unable to upload s3Key[%s]", s3Key), e);
    }
  }

  /**
   * Waits for every queued part, then either finalizes the multipart upload or aborts it. An abort discards the parts
   * uploaded so far and leaves no object at {@link #s3Key} and finally throws a {@link DruidException} to ensure
   * callers know an error occurred and that the object is not available for reading. The failure is an operator
   * concern rather than a Druid defect: it means S3 rejected the parts.
   */
  private void completeMultipartUpload()
  {
    final List<CompletedPart> pushResults = new ArrayList<>();
    Exception partUploadFailure = null;
    for (Future<UploadPartResponse> future : futures) {
      if (error) {
        future.cancel(true);
      }
      try {
        UploadPartResponse result = future.get(1, TimeUnit.HOURS);
        pushResults.add(CompletedPart.builder()
            .partNumber(pushResults.size() + 1)
            .eTag(result.eTag())
            .build());
      }
      catch (Exception e) {
        error = true;
        if (partUploadFailure == null) {
          partUploadFailure = e;
        }
        LOG.error(e, "Error in uploading part for upload ID [%s]", uploadId);
      }
    }

    try {
      boolean isAllPushSucceeded = !error && !pushResults.isEmpty() && futures.size() == pushResults.size();
      if (isAllPushSucceeded) {
        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(config.getBucket())
            .key(s3Key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder()
                .parts(pushResults)
                .build())
            .build();
        RetryUtils.retry(
            () -> s3.completeMultipartUpload(completeRequest),
            S3Utils.S3RETRY,
            config.getMaxRetry()
        );
      } else {
        AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
            .bucket(config.getBucket())
            .key(s3Key)
            .uploadId(uploadId)
            .build();
        RetryUtils.retry(
            () -> {
              s3.abortMultipartUpload(abortRequest);
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

    // If an error occurred during the upload of any part, we aborted the whole upload and there is nothing to read
    // downstream. We must throw here to indicate that the object was not written and avoid callers assuming that the
    // object is available for reading.
    if (error) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(
                              partUploadFailure,
                              "Aborted multipart upload[%s] for s3Key[%s]; no object was written",
                              uploadId,
                              s3Key
                          );
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
