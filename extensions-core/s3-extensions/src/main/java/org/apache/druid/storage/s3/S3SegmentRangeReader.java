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

package org.apache.druid.storage.s3;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.loading.SegmentRangeReader;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link SegmentRangeReader} backed by S3 HTTP {@code Range} requests. The segment is expected to be stored as raw
 * (unzipped) files under a common key prefix, i.e. the layout produced by {@code S3DataSegmentPusher.pushNoZip} where
 * each segment file is uploaded as {@code keyPrefix + file.getName()}. Each {@link #readRange} call resolves the
 * target object key as {@code keyPrefix + filename} and issues a closed byte-range GET
 * ({@code bytes=offset-(offset+length-1)}).
 * <p>
 * The returned stream is wrapped in a {@link RetryingInputStream} with the {@link S3Utils#S3RETRY} predicate, the
 * same retry policy {@link S3DataSegmentPuller} uses for full-segment downloads. Segment loading from deep storage
 * needs retry semantics built into the reader so callers don't each reinvent it. The retrying wrapper reopens at the
 * byte offset where it failed, so a transient mid-stream error becomes a fresh range request for the remaining bytes
 * rather than restarting the whole read.
 */
public class S3SegmentRangeReader implements SegmentRangeReader
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final String bucket;
  private final String keyPrefix;

  public S3SegmentRangeReader(ServerSideEncryptingAmazonS3 s3Client, String bucket, String keyPrefix)
  {
    this.s3Client = Preconditions.checkNotNull(s3Client, "s3Client");
    this.bucket = Preconditions.checkNotNull(bucket, "bucket");
    this.keyPrefix = Preconditions.checkNotNull(keyPrefix, "keyPrefix");
  }

  @Override
  public InputStream readRange(String filename, long offset, long length) throws IOException
  {
    Preconditions.checkNotNull(filename, "filename");
    Preconditions.checkArgument(offset >= 0, "offset must be non-negative, got [%s]", offset);
    Preconditions.checkArgument(length > 0, "length must be positive, got [%s]", length);

    return new RetryingInputStream<>(
        new RangeRequest(keyPrefix + filename, offset, length),
        new RangeOpenFunction(s3Client, bucket),
        S3Utils.S3RETRY,
        null
    );
  }

  /**
   * Immutable description of a range read. Held as the {@code object} of {@link RetryingInputStream} so retries can
   * reopen with knowledge of the original offset and length without rebuilding the request from scratch.
   */
  private static final class RangeRequest
  {
    final String objectKey;
    final long offset;
    final long length;

    RangeRequest(String objectKey, long offset, long length)
    {
      this.objectKey = objectKey;
      this.offset = offset;
      this.length = length;
    }
  }

  /**
   * Opens (or reopens, on retry) an S3 range read for a {@link RangeRequest}. The {@code start} argument is the
   * number of bytes already successfully consumed from the logical stream, so the absolute S3 byte range is
   * {@code [request.offset + start, request.offset + request.length - 1]}.
   */
  private static final class RangeOpenFunction implements ObjectOpenFunction<RangeRequest>
  {
    private final ServerSideEncryptingAmazonS3 s3Client;
    private final String bucket;

    RangeOpenFunction(ServerSideEncryptingAmazonS3 s3Client, String bucket)
    {
      this.s3Client = s3Client;
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
      final long absoluteStart = request.offset + start;
      final long absoluteEnd = request.offset + request.length - 1;
      if (absoluteStart > absoluteEnd) {
        // Logically nothing left to read, only reachable if a retry fires after the consumer drained the entire
        // range successfully, which shouldn't happen in practice. Returning empty keeps us robust either way.
        return new ByteArrayInputStream(new byte[0]);
      }
      final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
          .bucket(bucket)
          .key(request.objectKey)
          .range(AwsBytesRange.of(absoluteStart, absoluteEnd).getBytesRange());
      try {
        final InputStream s3Object = s3Client.getObject(requestBuilder);
        if (s3Object == null) {
          throw new ISE(
              "Failed to get s3 object for bucket[%s], key[%s], range[bytes=%d-%d]",
              bucket,
              request.objectKey,
              absoluteStart,
              absoluteEnd
          );
        }
        return s3Object;
      }
      catch (S3Exception e) {
        throw new IOException(e);
      }
    }
  }
}
