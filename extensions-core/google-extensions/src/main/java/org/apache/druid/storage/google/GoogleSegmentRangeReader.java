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

package org.apache.druid.storage.google;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.segment.loading.SegmentRangeReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link SegmentRangeReader} backed by Google Cloud Storage range reads. The segment is expected to be stored as raw
 * (unzipped) objects under a common path, i.e. the layout produced by {@code GoogleDataSegmentPusher.pushNoZip} where
 * each segment file is uploaded as {@code pathPrefix + file.getName()}. Each {@link #readRange} call resolves the
 * target object as {@code pathPrefix + filename} and opens a read channel limited to {@code [offset, offset + length)}.
 * <p>
 * The returned stream is wrapped in a {@link RetryingInputStream} with the {@link GoogleUtils#GOOGLE_RETRY} predicate,
 * the same retry policy {@link GoogleDataSegmentPuller} uses for full-segment downloads. The GCS client retries a
 * failed request on its own, but only the {@code RetryingInputStream} can resume a read that failed partway through:
 * it reopens at the byte offset already consumed, so a transient mid-stream error becomes a fresh range read for the
 * remaining bytes rather than a restart of the whole range.
 */
public class GoogleSegmentRangeReader implements SegmentRangeReader
{
  private final GoogleStorage storage;
  private final String bucket;
  private final String pathPrefix;

  public GoogleSegmentRangeReader(GoogleStorage storage, String bucket, String pathPrefix)
  {
    this.storage = Preconditions.checkNotNull(storage, "storage");
    this.bucket = Preconditions.checkNotNull(bucket, "bucket");
    this.pathPrefix = Preconditions.checkNotNull(pathPrefix, "pathPrefix");
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
        new RangeRequest(pathPrefix + filename, offset, length),
        new RangeOpenFunction(storage, bucket),
        GoogleUtils.GOOGLE_RETRY,
        null
    );
  }

  /**
   * Immutable description of a range read. Held as the {@code object} of {@link RetryingInputStream} so retries can
   * reopen with knowledge of the original offset and length without rebuilding the request from scratch.
   */
  private static final class RangeRequest
  {
    final String objectPath;
    final long offset;
    final long length;

    RangeRequest(String objectPath, long offset, long length)
    {
      this.objectPath = objectPath;
      this.offset = offset;
      this.length = length;
    }
  }

  /**
   * Opens (or reopens, on retry) a GCS range read for a {@link RangeRequest}. The {@code start} argument is the number
   * of bytes already successfully consumed from the logical stream, so the range still owed is
   * {@code [request.offset + start, request.offset + request.length)}.
   */
  private static final class RangeOpenFunction implements ObjectOpenFunction<RangeRequest>
  {
    private final GoogleStorage storage;
    private final String bucket;

    RangeOpenFunction(GoogleStorage storage, String bucket)
    {
      this.storage = storage;
      this.bucket = bucket;
    }

    @Override
    public InputStream open(RangeRequest request) throws IOException
    {
      return open(request, 0L);
    }

    @Override
    public InputStream open(RangeRequest request, long start) throws IOException
    {
      final long remaining = request.length - start;
      if (remaining <= 0) {
        // Logically nothing left to read, only reachable if a retry fires after the consumer drained the entire
        // range successfully, which shouldn't happen in practice. Returning empty keeps us robust either way.
        return new ByteArrayInputStream(new byte[0]);
      }
      // A StorageException is deliberately left to propagate as it is. GoogleUtils.isRetryable inspects only the
      // throwable it is handed (it does not walk the cause chain) and answers `instanceof IOException` for anything
      // it does not recognize, so wrapping this would report every failure as retryable and spend ten backing-off
      // attempts on a permission error. Raw, the predicate calls StorageException#isRetryable and decides correctly;
      // RetryingInputStream hands the caller an IOException around it either way.
      return storage.getInputStream(bucket, request.objectPath, request.offset + start, remaining);
    }
  }
}
