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

package org.apache.druid.storage.azure;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.segment.loading.SegmentRangeReader;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link SegmentRangeReader} backed by Azure blob range reads. The segment is expected to be stored as raw (unzipped)
 * blobs under a common path, i.e. the layout produced by {@code AzureDataSegmentPusher.pushNoZip} where each segment
 * file is uploaded as {@code blobPathPrefix + file.getName()}. Each {@link #readRange} call resolves the target blob
 * as {@code blobPathPrefix + filename} and opens a stream over {@code [offset, offset + length)}.
 * <p>
 * The returned stream is wrapped in a {@link RetryingInputStream} with the {@link AzureUtils#AZURE_RETRY} predicate,
 * the same retry policy {@link AzureDataSegmentPuller} uses for full-segment downloads. The Azure client retries a
 * failed request on its own, but only the {@code RetryingInputStream} can resume a read that failed partway through:
 * it reopens at the byte offset already consumed, so a transient mid-stream error becomes a fresh range read for the
 * remaining bytes rather than a restart of the whole range.
 */
public class AzureSegmentRangeReader implements SegmentRangeReader
{
  private final AzureStorage azureStorage;
  private final String containerName;
  private final String blobPathPrefix;
  @Nullable
  private final Integer maxTries;

  public AzureSegmentRangeReader(
      AzureStorage azureStorage,
      String containerName,
      String blobPathPrefix,
      @Nullable Integer maxTries
  )
  {
    this.azureStorage = Preconditions.checkNotNull(azureStorage, "azureStorage");
    this.containerName = Preconditions.checkNotNull(containerName, "containerName");
    this.blobPathPrefix = Preconditions.checkNotNull(blobPathPrefix, "blobPathPrefix");
    this.maxTries = maxTries;
  }

  @Override
  public InputStream readRange(String filename, long offset, long length) throws IOException
  {
    Preconditions.checkNotNull(filename, "filename");
    Preconditions.checkArgument(offset >= 0, "offset must be non-negative, got [%s]", offset);
    Preconditions.checkArgument(length >= 0, "length must be non-negative, got [%s]", length);

    if (length == 0) {
      // SegmentFileBuilderV10 allows zero-length internal-file entries, short-circuit
      return new ByteArrayInputStream(new byte[0]);
    }
    return new RetryingInputStream<>(
        new RangeRequest(blobPathPrefix + filename, offset, length),
        new RangeOpenFunction(azureStorage, containerName, maxTries),
        AzureUtils.AZURE_RETRY,
        null
    );
  }

  /**
   * Immutable description of a range read. Held as the {@code object} of {@link RetryingInputStream} so retries can
   * reopen with knowledge of the original offset and length without rebuilding the request from scratch.
   */
  private static final class RangeRequest
  {
    final String blobPath;
    final long offset;
    final long length;

    RangeRequest(String blobPath, long offset, long length)
    {
      this.blobPath = blobPath;
      this.offset = offset;
      this.length = length;
    }
  }

  /**
   * Opens (or reopens, on retry) an Azure range read for a {@link RangeRequest}. The {@code start} argument is the
   * number of bytes already successfully consumed from the logical stream, so the range still owed is
   * {@code [request.offset + start, request.offset + request.length)}.
   */
  private static final class RangeOpenFunction implements ObjectOpenFunction<RangeRequest>
  {
    private final AzureStorage azureStorage;
    private final String containerName;
    @Nullable
    private final Integer maxTries;

    RangeOpenFunction(AzureStorage azureStorage, String containerName, @Nullable Integer maxTries)
    {
      this.azureStorage = azureStorage;
      this.containerName = containerName;
      this.maxTries = maxTries;
    }

    @Override
    public InputStream open(RangeRequest request)
    {
      return open(request, 0L);
    }

    @Override
    public InputStream open(RangeRequest request, long start)
    {
      final long remaining = request.length - start;
      if (remaining <= 0) {
        // Logically nothing left to read, only reachable if a retry fires after the consumer drained the entire
        // range successfully, which shouldn't happen in practice. Returning empty keeps us robust either way.
        return new ByteArrayInputStream(new byte[0]);
      }
      // A BlobStorageException is deliberately left unwrapped. AzureUtils.AZURE_RETRY walks the cause chain but tests
      // for IOException before BlobStorageException within each link, so wrapping this in an IOException would make
      // every failure look retryable and turn a 403 into ten backing-off attempts. Raw, the predicate reads the status
      // code and decides correctly; RetryingInputStream then hands the caller an IOException around it either way.
      return azureStorage.getBlockBlobInputStream(
          request.offset + start,
          remaining,
          containerName,
          request.blobPath,
          maxTries
      );
    }
  }
}
