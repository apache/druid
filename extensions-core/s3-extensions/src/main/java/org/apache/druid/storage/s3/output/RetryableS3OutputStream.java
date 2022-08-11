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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.io.CountingOutputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
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
  private static final Logger LOG = new Logger(RetryableS3OutputStream.class);

  private final S3OutputConfig config;
  private final ServerSideEncryptingAmazonS3 s3;
  private final String s3Key;
  private final String uploadId;
  private final File chunkStorePath;
  private final long chunkSize;

  private final List<PartETag> pushResults = new ArrayList<>();
  private final byte[] singularBuffer = new byte[1];

  // metric
  private final Stopwatch pushStopwatch;

  private Chunk currentChunk;
  private int nextChunkId = 1; // multipart upload requires partNumber to be in the range between 1 and 10000
  private int numChunksPushed;
  /**
   * Total size of all chunks. This size is updated whenever the chunk is ready for push,
   * not when {@link #write(byte[], int, int)} is called.
   */
  private long resultsSize;

  /**
   * A flag indicating whether there was an upload error.
   * This flag is tested in {@link #close()} to determine whether it needs to upload the current chunk or not.
   */
  private boolean error;
  private boolean closed;

  public RetryableS3OutputStream(
      S3OutputConfig config,
      ServerSideEncryptingAmazonS3 s3,
      String s3Key
  ) throws IOException
  {

    this(config, s3, s3Key, true);
  }

  @VisibleForTesting
  protected RetryableS3OutputStream(
      S3OutputConfig config,
      ServerSideEncryptingAmazonS3 s3,
      String s3Key,
      boolean chunkValidation
  ) throws IOException
  {
    this.config = config;
    this.s3 = s3;
    this.s3Key = s3Key;

    final InitiateMultipartUploadResult result = s3.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(config.getBucket(), s3Key)
    );
    this.uploadId = result.getUploadId();
    this.chunkStorePath = new File(config.getTempDir(), uploadId + UUID.randomUUID());
    FileUtils.mkdirp(this.chunkStorePath);
    this.chunkSize = config.getChunkSize();
    this.pushStopwatch = Stopwatch.createUnstarted();
    this.pushStopwatch.reset();

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
    try {
      if (chunk.length() > 0) {
        resultsSize += chunk.length();
        if (resultsSize > config.getMaxResultsSize()) {
          throw new IOE("Exceeded max results size [%s]", config.getMaxResultsSize());
        }

        pushStopwatch.start();
        pushResults.add(push(chunk));
        pushStopwatch.stop();
        numChunksPushed++;
      }
    }
    finally {
      if (!chunk.delete()) {
        LOG.warn("Failed to delete chunk [%s]", chunk.getAbsolutePath());
      }
    }
  }

  private PartETag push(Chunk chunk) throws IOException
  {
    try {
      return RetryUtils.retry(
          () -> uploadPartIfPossible(uploadId, config.getBucket(), s3Key, chunk),
          S3Utils.S3RETRY,
          config.getMaxRetry()
      );
    }
    catch (AmazonServiceException e) {
      throw new IOException(e);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private PartETag uploadPartIfPossible(
      String uploadId,
      String bucket,
      String key,
      Chunk chunk
  )
  {
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(resultsSize);
    final UploadPartRequest uploadPartRequest = new UploadPartRequest()
        .withUploadId(uploadId)
        .withBucketName(bucket)
        .withKey(key)
        .withFile(chunk.file)
        .withPartNumber(chunk.id)
        .withPartSize(chunk.length());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Pushing chunk [%s] to bucket[%s] and key[%s].", chunk, bucket, key);
    }
    UploadPartResult uploadResult = s3.uploadPart(uploadPartRequest);
    return uploadResult.getPartETag();
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
      // This should be emitted as a metric
      LOG.info("Total push time: [%d] ms", pushStopwatch.elapsed(TimeUnit.MILLISECONDS));
    });

    closer.register(() -> org.apache.commons.io.FileUtils.forceDelete(chunkStorePath));

    closer.register(() -> {
      try {
        if (resultsSize > 0 && isAllPushSucceeded()) {
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
        throw new IOException(e);
      }
    });

    try (Closer ignored = closer) {
      if (!error) {
        pushCurrentChunk();
      }
    }
  }

  private boolean isAllPushSucceeded()
  {
    return !error && !pushResults.isEmpty() && numChunksPushed == pushResults.size();
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

    private boolean delete()
    {
      return file.delete();
    }

    private String getAbsolutePath()
    {
      return file.getAbsolutePath();
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
