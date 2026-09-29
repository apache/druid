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

import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.io.ByteStreams;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AzureSegmentRangeReaderTest
{
  private static final String CONTAINER = "test-container";
  private static final String BLOB_PATH_PREFIX = "ds/20240101T000000.000Z_20240102T000000.000Z/0/0/";
  private static final int MAX_TRIES = 3;

  @Mock
  private AzureStorage azureStorage;

  private AzureSegmentRangeReader reader;

  @BeforeEach
  public void setUp()
  {
    reader = new AzureSegmentRangeReader(azureStorage, CONTAINER, BLOB_PATH_PREFIX, MAX_TRIES);
  }

  @Test
  public void testReadRangeOpensBlobPathPrefixPlusFilenameAtOffsetAndLength() throws IOException
  {
    stubRange(new byte[0]);

    try (InputStream ignored = reader.readRange("druid.segment", 100, 250)) {
      // open is performed in the RetryingInputStream constructor; the blob read should have already happened.
    }

    verify(azureStorage).getBlockBlobInputStream(
        100L,
        250L,
        CONTAINER,
        BLOB_PATH_PREFIX + "druid.segment",
        MAX_TRIES
    );
  }

  @Test
  public void testReadRangeBuildsDifferentBlobPathsForDifferentFilenames() throws IOException
  {
    stubRange(new byte[0]);

    reader.readRange("file-a", 0, 16).close();
    reader.readRange("file-b", 0, 16).close();

    final ArgumentCaptor<String> blobPathCaptor = ArgumentCaptor.forClass(String.class);
    verify(azureStorage, times(2)).getBlockBlobInputStream(
        anyLong(),
        eq(16L),
        eq(CONTAINER),
        blobPathCaptor.capture(),
        eq(MAX_TRIES)
    );
    assertEquals(BLOB_PATH_PREFIX + "file-a", blobPathCaptor.getAllValues().get(0));
    assertEquals(BLOB_PATH_PREFIX + "file-b", blobPathCaptor.getAllValues().get(1));
  }

  @Test
  public void testReadRangeFailsFastOnNonRetryableBlobStorageException()
  {
    // 403 isn't retryable per AzureUtils.AZURE_RETRY, so open fails once and RetryingInputStream surfaces it as
    // IOException(BlobStorageException). The exception must reach the predicate unwrapped for that to hold: AZURE_RETRY
    // checks IOException before BlobStorageException within each link of the cause chain, so an IOException wrapper
    // here would report every failure as retryable and spend ten backing-off attempts on a permission error.
    final HttpResponse httpResponse = org.mockito.Mockito.mock(HttpResponse.class);
    when(httpResponse.getStatusCode()).thenReturn(403);
    when(azureStorage.getBlockBlobInputStream(anyLong(), anyLong(), eq(CONTAINER), eq(BLOB_PATH_PREFIX + "f"), eq(MAX_TRIES)))
        .thenThrow(new BlobStorageException("denied", httpResponse, null));

    final IOException thrown = assertThrows(IOException.class, () -> reader.readRange("f", 0, 1));
    assertSame(BlobStorageException.class, thrown.getCause().getClass());
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

    when(azureStorage.getBlockBlobInputStream(anyLong(), anyLong(), eq(CONTAINER), eq(BLOB_PATH_PREFIX + "f"), eq(MAX_TRIES)))
        .thenReturn(failingAfter(firstChunk))
        .thenReturn(new ByteArrayInputStream(secondChunk));

    final byte[] read;
    try (InputStream stream = reader.readRange("f", 100, 10)) {
      read = ByteStreams.toByteArray(stream);
    }
    assertArrayEquals(all, read);

    final ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
    final ArgumentCaptor<Long> lengthCaptor = ArgumentCaptor.forClass(Long.class);
    verify(azureStorage, times(2)).getBlockBlobInputStream(
        offsetCaptor.capture(),
        lengthCaptor.capture(),
        eq(CONTAINER),
        eq(BLOB_PATH_PREFIX + "f"),
        eq(MAX_TRIES)
    );
    // First: the full requested range
    assertEquals(100L, offsetCaptor.getAllValues().get(0));
    assertEquals(10L, lengthCaptor.getAllValues().get(0));
    // Retry: resume at (offset + bytes already consumed), asking only for what is still owed
    assertEquals(104L, offsetCaptor.getAllValues().get(1));
    assertEquals(6L, lengthCaptor.getAllValues().get(1));
  }

  @Test
  public void testReadRangeReturnsEmptyStreamForZeroLengthWithoutContactingAzure() throws IOException
  {
    // SegmentFileBuilderV10 allows zero-length internal-file entries; readRange must accept length=0 and return an
    // empty stream without opening a blob (a zero-length BlobRange would read to the end of the blob instead).
    try (InputStream stream = reader.readRange("f", 100, 0)) {
      assertEquals(-1, stream.read());
    }
    verifyNoInteractions(azureStorage);
  }

  @Test
  public void testReadRangeRejectsNegativeOffset()
  {
    assertThrows(IllegalArgumentException.class, () -> reader.readRange("f", -1, 16));
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
    // returning AzureSegmentRangeReader from openRangeReader().
    final SegmentRangeReader downcast = reader;
    assertSame(reader, downcast);
  }

  private void stubRange(byte[] bytes)
  {
    when(azureStorage.getBlockBlobInputStream(anyLong(), anyLong(), eq(CONTAINER), org.mockito.ArgumentMatchers.anyString(), eq(MAX_TRIES)))
        .thenAnswer(invocation -> new ByteArrayInputStream(bytes));
  }

  /**
   * Returns a stream that delivers the given {@code head} bytes successfully, then raises a bare {@link IOException}
   * (no cause) on the next read — which {@link AzureUtils#AZURE_RETRY} treats as retryable, so
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
