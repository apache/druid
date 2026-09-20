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

import com.google.common.io.ByteStreams;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3SegmentRangeReaderTest
{
  private static final String BUCKET = "test-bucket";
  private static final String KEY_PREFIX = "ds/2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z/0/0/";

  @Mock
  private ServerSideEncryptingAmazonS3 s3Client;

  private S3SegmentRangeReader reader;

  @BeforeEach
  public void setUp()
  {
    reader = new S3SegmentRangeReader(s3Client, BUCKET, KEY_PREFIX);
  }

  @Test
  public void testReadRangeIssuesClosedRangeGetWithKeyPrefixPlusFilename() throws IOException
  {
    when(s3Client.getObject(any(GetObjectRequest.Builder.class))).thenReturn(stubResponse(new byte[0]));

    try (InputStream ignored = reader.readRange("000000.smoosh", 100, 250)) {
      // open is performed in the RetryingInputStream constructor; the GetObject call should have already happened.
    }

    final GetObjectRequest request = captureRequest();
    assertEquals(BUCKET, request.bucket());
    assertEquals(KEY_PREFIX + "000000.smoosh", request.key());
    // closed range: bytes=offset-(offset+length-1)
    assertEquals("bytes=100-349", request.range());
  }

  @Test
  public void testReadRangeBuildsDifferentKeysForDifferentFilenames() throws IOException
  {
    when(s3Client.getObject(any(GetObjectRequest.Builder.class))).thenReturn(stubResponse(new byte[0]));

    reader.readRange("file-a", 0, 16).close();
    reader.readRange("file-b", 0, 16).close();

    final ArgumentCaptor<GetObjectRequest.Builder> builderCaptor =
        ArgumentCaptor.forClass(GetObjectRequest.Builder.class);
    verify(s3Client, times(2)).getObject(builderCaptor.capture());
    assertEquals(KEY_PREFIX + "file-a", builderCaptor.getAllValues().get(0).build().key());
    assertEquals(KEY_PREFIX + "file-b", builderCaptor.getAllValues().get(1).build().key());
  }

  @Test
  public void testReadRangeWithSingleByteUsesInclusiveRange() throws IOException
  {
    when(s3Client.getObject(any(GetObjectRequest.Builder.class))).thenReturn(stubResponse(new byte[0]));

    reader.readRange("f", 42, 1).close();

    // bytes=offset-offset (length=1 → end = offset+0)
    assertEquals("bytes=42-42", captureRequest().range());
  }

  @Test
  public void testReadRangeWrapsNonRetryableS3ExceptionAsIOException()
  {
    // 403 isn't retryable per AWSClientUtil.isClientExceptionRecoverable → RetryingInputStream's open fails once,
    // S3RETRY says no, the IOException(S3Exception) is propagated as-is by Throwables.propagateIfInstanceOf.
    when(s3Client.getObject(any(GetObjectRequest.Builder.class)))
        .thenThrow((S3Exception) S3Exception.builder().message("denied").statusCode(403).build());

    final IOException thrown = assertThrows(IOException.class, () -> reader.readRange("f", 0, 1));
    assertSame(S3Exception.class, thrown.getCause().getClass());
  }

  @Test
  public void testReadRangeRetriesMidStreamFromBytesAlreadyConsumed() throws IOException
  {
    // First range read delivers the first 4 bytes then errors mid-stream with a retryable IOException.
    // RetryingInputStream should reopen at the next byte (request.offset + 4), exercising the offset math in
    // RangeOpenFunction.open(request, start). NOTE: this test sleeps ~1s because RetryingInputStream's first retry
    // uses RetryUtils.BASE_SLEEP_MILLIS exponential backoff (no @VisibleForTesting hook is accessible from here).
    final byte[] firstChunk = {0x01, 0x02, 0x03, 0x04};
    final byte[] secondChunk = {0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
    final byte[] all = new byte[firstChunk.length + secondChunk.length];
    System.arraycopy(firstChunk, 0, all, 0, firstChunk.length);
    System.arraycopy(secondChunk, 0, all, firstChunk.length, secondChunk.length);

    when(s3Client.getObject(any(GetObjectRequest.Builder.class)))
        .thenReturn(stubResponseFailingAfter(firstChunk))
        .thenReturn(stubResponse(secondChunk));

    final byte[] read;
    try (InputStream stream = reader.readRange("f", 100, 10)) {
      read = ByteStreams.toByteArray(stream);
    }
    assertArrayEquals(all, read);

    final ArgumentCaptor<GetObjectRequest.Builder> builderCaptor =
        ArgumentCaptor.forClass(GetObjectRequest.Builder.class);
    verify(s3Client, times(2)).getObject(builderCaptor.capture());
    final List<GetObjectRequest> requests = Arrays.asList(
        builderCaptor.getAllValues().get(0).build(),
        builderCaptor.getAllValues().get(1).build()
    );
    // First: full requested range
    assertEquals("bytes=100-109", requests.get(0).range());
    // Retry: resume at (offset + bytes-already-consumed) through original end
    assertEquals("bytes=104-109", requests.get(1).range());
  }

  @Test
  public void testReadRangeRejectsNegativeOffset()
  {
    assertThrows(IllegalArgumentException.class, () -> reader.readRange("f", -1, 16));
  }

  @Test
  public void testReadRangeReturnsEmptyStreamForZeroLengthWithoutContactingS3() throws IOException
  {
    // SegmentFileBuilderV10 allows zero-length internal-file entries; readRange must accept length=0 and return an
    // empty stream without issuing an S3 GET (a closed range of bytes=N-(N-1) would 416).
    try (InputStream stream = reader.readRange("f", 100, 0)) {
      assertEquals(-1, stream.read());
    }
    verifyNoInteractions(s3Client);
  }

  @Test
  public void testReadRangeRejectsNegativeLength()
  {
    assertThrows(IllegalArgumentException.class, () -> reader.readRange("f", 0, -1));
  }

  @Test
  public void testImplementsSegmentRangeReader()
  {
    // Compile-time and runtime guard: ensure the implements relationship survives refactors so callers can rely on
    // returning S3SegmentRangeReader from openRangeReader().
    final SegmentRangeReader downcast = reader;
    assertSame(reader, downcast);
  }

  private GetObjectRequest captureRequest()
  {
    final ArgumentCaptor<GetObjectRequest.Builder> builderCaptor =
        ArgumentCaptor.forClass(GetObjectRequest.Builder.class);
    verify(s3Client).getObject(builderCaptor.capture());
    return builderCaptor.getValue().build();
  }

  private static ResponseInputStream<GetObjectResponse> stubResponse(byte[] bytes)
  {
    return new ResponseInputStream<>(
        GetObjectResponse.builder().build(),
        new ByteArrayInputStream(bytes)
    );
  }

  /**
   * Returns a stream that delivers the given {@code head} bytes successfully, then raises a bare {@link IOException}
   * (no cause) on the next read — which {@link S3Utils#S3RETRY} treats as retryable, so {@link
   * org.apache.druid.data.input.impl.RetryingInputStream} will close this delegate and call the open function again
   * with {@code start = head.length}.
   */
  private static ResponseInputStream<GetObjectResponse> stubResponseFailingAfter(byte[] head)
  {
    final InputStream delegate = new InputStream()
    {
      private final ByteArrayInputStream src = new ByteArrayInputStream(head);

      @Override
      public int read() throws IOException
      {
        final int b = src.read();
        if (b == -1) {
          throw new IOException("simulated mid-stream failure");
        }
        return b;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException
      {
        final int n = src.read(b, off, len);
        if (n == -1) {
          throw new IOException("simulated mid-stream failure");
        }
        return n;
      }
    };
    return new ResponseInputStream<>(GetObjectResponse.builder().build(), delegate);
  }
}
