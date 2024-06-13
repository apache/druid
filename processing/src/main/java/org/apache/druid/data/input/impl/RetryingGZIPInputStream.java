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

package org.apache.druid.data.input.impl;

import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class RetryingGZIPInputStream extends InputStream
{
  private static final Logger log = new Logger(RetryingGZIPInputStream.class);

  private InputStream delegate;
  private final InputStream in;
  private long startOffset = 0;

  private static final Predicate<Throwable> RETRY_CONDITION = e -> e.getMessage().equals("Unexpected end of ZLIB input stream");
  private static final int GZIP_BUFFER_SIZE = 8192; // Default is 512
  private static final int MAX_TRIES = 10;

  // Used in tests to disable waiting.
  private final boolean doWait;

  public RetryingGZIPInputStream(InputStream delegate, InputStream in)
  {
    this(delegate, in, true);
  }

  public RetryingGZIPInputStream(InputStream delegate, InputStream in, boolean doWait)
  {
    this.delegate = delegate;
    this.in = in;
    this.doWait = doWait;
  }

  @Override
  public int read() throws IOException
  {
    for (int nTry = 0; nTry < MAX_TRIES; nTry++) {
      try {
        int numBytes = delegate.read();
        startOffset += numBytes;
        return numBytes;
      }
      catch (Throwable t) {
        waitOrThrow(t, nTry);
      }
    }

    // Can't happen, because the final waitOrThrow would have thrown.
    throw new IllegalStateException();
  }

  @Override
  public int read(byte[] b) throws IOException
  {
    for (int nTry = 0; nTry < MAX_TRIES; nTry++) {
      try {
        int numBytes = delegate.read(b);
        startOffset += numBytes;
        return numBytes;
      }
      catch (Throwable t) {
        waitOrThrow(t, nTry);
      }
    }

    // Can't happen, because the final waitOrThrow would have thrown.
    throw new IllegalStateException();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException
  {
    for (int nTry = 0; nTry < MAX_TRIES; nTry++) {
      try {
        int numBytes = delegate.read(b, off, len);
        startOffset += numBytes;
        return numBytes;
      }
      catch (Throwable t) {
        waitOrThrow(t, nTry);
      }
    }

    // Can't happen, because the final waitOrThrow would have thrown.
    throw new IllegalStateException();
  }

  private void waitOrThrow(Throwable t, int nTry) throws IOException
  {
    log.info("Encountered an error while reading the input stream, attempt number: %d, error: %s", nTry, t.getMessage());
    try {
      delegate.close();
    }
    catch (IOException e) {
      // ignore this exception
      log.warn(e, "Error while closing the delegate input stream. Discarding.");
    }
    finally {
      delegate = null;
    }

    final int nextTry = nTry + 1;

    if (nextTry < MAX_TRIES && RETRY_CONDITION.apply(t)) {
      try {
        // Pause for some time and then re-open the input stream.
        final String message = StringUtils.format("Stream interrupted at position [%d]", startOffset);

        if (doWait) {
          RetryUtils.awaitNextRetry(t, message, nextTry, MAX_TRIES, false);
        }
        openWithRetry(startOffset);
      }
      catch (InterruptedException | IOException e) {
        t.addSuppressed(e);
        RetryingInputStreamUtils.throwAsIOException(t);
      }
    } else {
      RetryingInputStreamUtils.throwAsIOException(t);
    }
  }

  private void openWithRetry(final long offset) throws IOException
  {
    for (int nTry = 0; nTry < MAX_TRIES; nTry++) {
      try {
        // Reset the underlying input stream as we need to start from the beginning to avoid mismatch in
        // GZIP header magic number in GZIPInputStream#readHeader.
        in.reset();

        delegate = createGZIPInputStream(in);

        // Skip the already processed bytes.
        delegate.skip(startOffset);
        break;
      }
      catch (Throwable t) {
        RetryingInputStreamUtils.handleInputStreamOpenError(t, RETRY_CONDITION, nTry, MAX_TRIES, offset, doWait);
      }
    }
  }

  public static GZIPInputStream createGZIPInputStream(InputStream in) throws IOException
  {
    return new GZIPInputStream(
        new FilterInputStream(in)
        {
          @Override
          public int available() throws IOException
          {
            final int otherAvailable = super.available();
            // Hack. Docs say available() should return an estimate,
            // so we estimate about 1KiB to work around available == 0 bug in GZIPInputStream
            return otherAvailable == 0 ? 1 << 10 : otherAvailable;
          }
        },
        GZIP_BUFFER_SIZE
    );
  }
}
