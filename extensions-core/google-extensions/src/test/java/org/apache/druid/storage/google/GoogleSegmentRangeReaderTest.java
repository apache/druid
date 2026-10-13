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

import com.google.cloud.storage.StorageException;
import com.google.common.io.ByteStreams;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GoogleSegmentRangeReaderTest extends EasyMockSupport
{
  private static final String BUCKET = "test-bucket";
  private static final String PATH_PREFIX = "ds/2024-01-01T00:00:00.000Z_2024-01-02T00:00:00.000Z/0/0/";

  private GoogleStorage storage;
  private GoogleSegmentRangeReader reader;

  @BeforeEach
  public void setUp()
  {
    storage = createMock(GoogleStorage.class);
    reader = new GoogleSegmentRangeReader(storage, BUCKET, PATH_PREFIX);
  }

  @Test
  public void testReadRangeOpensPathPrefixPlusFilenameAtOffsetAndLength() throws IOException
  {
    EasyMock.expect(storage.getInputStream(BUCKET, PATH_PREFIX + "druid.segment", 100L, 250L))
            .andReturn(new ByteArrayInputStream(new byte[0]));

    replayAll();

    try (InputStream ignored = reader.readRange("druid.segment", 100, 250)) {
      // open is performed in the RetryingInputStream constructor; the read channel should have already been opened.
    }

    verifyAll();
  }

  @Test
  public void testReadRangeBuildsDifferentPathsForDifferentFilenames() throws IOException
  {
    final Capture<String> pathCapture = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(storage.getInputStream(EasyMock.eq(BUCKET), EasyMock.capture(pathCapture), EasyMock.eq(0L), EasyMock.eq(16L)))
            .andReturn(new ByteArrayInputStream(new byte[0]))
            .times(2);

    replayAll();

    reader.readRange("file-a", 0, 16).close();
    reader.readRange("file-b", 0, 16).close();

    verifyAll();

    assertEquals(PATH_PREFIX + "file-a", pathCapture.getValues().get(0));
    assertEquals(PATH_PREFIX + "file-b", pathCapture.getValues().get(1));
  }

  @Test
  public void testReadRangeFailsFastOnNonRetryableStorageException() throws IOException
  {
    // 403 isn't retryable per StorageException#isRetryable, so open fails once and RetryingInputStream surfaces it as
    // IOException(StorageException). The exception must reach the predicate unwrapped for that to hold:
    // GoogleUtils.isRetryable inspects only the throwable it is handed and answers `instanceof IOException` for
    // anything it doesn't recognize, so an IOException wrapper here would report a permission error as retryable and
    // spend ten backing-off attempts on it.
    EasyMock.expect(storage.getInputStream(BUCKET, PATH_PREFIX + "f", 0L, 1L))
            .andThrow(new StorageException(403, "denied"));

    replayAll();

    final IOException thrown = assertThrows(IOException.class, () -> reader.readRange("f", 0, 1));
    assertSame(StorageException.class, thrown.getCause().getClass());

    verifyAll();
  }

  @Test
  public void testReadRangeRetriesMidStreamFromBytesAlreadyConsumed() throws IOException
  {
    // First range read delivers the first 4 bytes then errors mid-stream with a retryable IOException.
    // RetryingInputStream should reopen for the bytes still owed, exercising the offset math in
    // RangeOpenFunction.open(request, start). NOTE: this test sleeps ~1s because RetryingInputStream's first retry
    // uses RetryUtils.BASE_SLEEP_MILLIS exponential backoff (no @VisibleForTesting hook is accessible from here).
    final byte[] firstChunk = {0x01, 0x02, 0x03, 0x04};
    final byte[] secondChunk = {0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
    final byte[] all = new byte[firstChunk.length + secondChunk.length];
    System.arraycopy(firstChunk, 0, all, 0, firstChunk.length);
    System.arraycopy(secondChunk, 0, all, firstChunk.length, secondChunk.length);

    // First: the full requested range
    EasyMock.expect(storage.getInputStream(BUCKET, PATH_PREFIX + "f", 100L, 10L))
            .andReturn(failingAfter(firstChunk));
    // Retry: resume at (offset + bytes already consumed), asking only for what is still owed
    EasyMock.expect(storage.getInputStream(BUCKET, PATH_PREFIX + "f", 104L, 6L))
            .andReturn(new ByteArrayInputStream(secondChunk));

    replayAll();

    final byte[] read;
    try (InputStream stream = reader.readRange("f", 100, 10)) {
      read = ByteStreams.toByteArray(stream);
    }
    assertArrayEquals(all, read);

    verifyAll();
  }

  @Test
  public void testReadRangeReturnsEmptyStreamForZeroLengthWithoutContactingStorage() throws IOException
  {
    // SegmentFileBuilderV10 allows zero-length internal-file entries; readRange must accept length=0 and return an
    // empty stream without opening a read channel. Replaying with no expectations fails the test on any call.
    replayAll();

    try (InputStream stream = reader.readRange("f", 100, 0)) {
      assertEquals(-1, stream.read());
    }

    verifyAll();
  }

  @Test
  public void testReadRangeRejectsNegativeOffset()
  {
    replayAll();
    assertThrows(IllegalArgumentException.class, () -> reader.readRange("f", -1, 16));
    verifyAll();
  }

  @Test
  public void testReadRangeRejectsNegativeLength()
  {
    replayAll();
    assertThrows(IllegalArgumentException.class, () -> reader.readRange("f", 0, -1));
    verifyAll();
  }

  @Test
  public void testRetryPredicateCannotSeeThroughAnIOExceptionWrapper()
  {
    // Guards the reason RangeOpenFunction hands StorageException to RetryingInputStream unwrapped. If isRetryable ever
    // starts walking the cause chain, wrapping becomes safe and the comment at that throw site should be revisited.
    final StorageException nonRetryable = new StorageException(403, "denied");
    assertFalse(GoogleUtils.isRetryable(nonRetryable));
    assertTrue(GoogleUtils.isRetryable(new IOException(nonRetryable)));
  }

  @Test
  public void testImplementsSegmentRangeReader()
  {
    // Compile-time and runtime guard: ensure the implements relationship survives refactors so callers can rely on
    // returning GoogleSegmentRangeReader from openRangeReader().
    final SegmentRangeReader downcast = reader;
    assertSame(reader, downcast);
  }

  /**
   * Returns a stream that delivers the given {@code head} bytes successfully, then raises a bare {@link IOException}
   * (no cause) on the next read — which {@link GoogleUtils#isRetryable} treats as retryable, so
   * {@link org.apache.druid.data.input.impl.RetryingInputStream} will close this delegate and call the open function
   * again with {@code start = head.length}.
   */
  private static InputStream failingAfter(byte[] head)
  {
    return new FilterInputStream(new ByteArrayInputStream(head))
    {
      @Override
      public int read(byte[] b, int off, int len) throws IOException
      {
        final int read = super.read(b, off, len);
        if (read < 0) {
          throw new IOException("mid-stream failure");
        }
        return read;
      }

      @Override
      public int read() throws IOException
      {
        final int read = super.read();
        if (read < 0) {
          throw new IOException("mid-stream failure");
        }
        return read;
      }
    };
  }
}
