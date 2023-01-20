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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.io.CountingInputStream;
import org.apache.druid.data.input.impl.prefetch.Fetcher;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class is used by {@link Fetcher} when prefetch is disabled. It's responsible for re-opening the underlying input
 * stream for the input object on the given {@link #retryCondition}.
 *
 * @param <T> object type
 */
public class RetryingInputStream<T> extends InputStream
{
  private static final Logger log = new Logger(RetryingInputStream.class);

  private final T object;
  private final ObjectOpenFunction<T> objectOpenFunction;
  private final Predicate<Throwable> retryCondition;
  private final int maxTries;

  private CountingInputStream delegate;
  private long startOffset;

  // Used in tests to disable waiting.
  private boolean doWait;

  /**
   * @param object             The object entity to open
   * @param objectOpenFunction How to open the object
   * @param retryCondition     A predicate on a throwable to indicate if stream should retry.
   * @param maxTries           The maximum times to try. Defaults to {@link RetryUtils#DEFAULT_MAX_TRIES} when null
   *
   * @throws IOException
   */
  public RetryingInputStream(
      T object,
      ObjectOpenFunction<T> objectOpenFunction,
      Predicate<Throwable> retryCondition,
      @Nullable Integer maxTries
  ) throws IOException
  {
    this.object = Preconditions.checkNotNull(object, "object");
    this.objectOpenFunction = Preconditions.checkNotNull(objectOpenFunction, "objectOpenFunction");
    this.retryCondition = Preconditions.checkNotNull(retryCondition, "retryCondition");
    this.maxTries = maxTries == null ? RetryUtils.DEFAULT_MAX_TRIES : maxTries;
    this.delegate = new CountingInputStream(objectOpenFunction.open(object));
    this.doWait = true;

    if (this.maxTries <= 1) {
      throw new IAE("maxTries must be greater than 1");
    }
  }

  private void openIfNeeded() throws IOException
  {
    if (delegate == null) {
      delegate = new CountingInputStream(objectOpenFunction.open(object, startOffset));
    }
  }

  private void waitOrThrow(Throwable t, int nTry) throws IOException
  {
    // Update startOffset first, since we're about to close and null out the delegate.
    startOffset += delegate.getCount();

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

    if (nextTry < maxTries && retryCondition.apply(t)) {
      try {
        // Pause for some time and then re-open the input stream.
        final String message = StringUtils.format("Stream interrupted at position [%d]", startOffset);

        if (doWait) {
          RetryUtils.awaitNextRetry(t, message, nextTry, maxTries, false);
        }

        delegate = new CountingInputStream(objectOpenFunction.open(object, startOffset));
      }
      catch (InterruptedException | IOException e) {
        t.addSuppressed(e);
        throwAsIOException(t);
      }
    } else {
      throwAsIOException(t);
    }
  }

  private static void throwAsIOException(Throwable t) throws IOException
  {
    Throwables.propagateIfInstanceOf(t, IOException.class);
    throw new IOException(t);
  }

  @Override
  public int read() throws IOException
  {
    openIfNeeded();

    for (int nTry = 0; nTry < maxTries; nTry++) {
      try {
        return delegate.read();
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
    // Full implementation, rather than calling to read(b, 0, b.len), just to ensure we use the
    // corresponding method of our delegate.

    openIfNeeded();

    for (int nTry = 0; nTry < maxTries; nTry++) {
      try {
        return delegate.read(b);
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
    openIfNeeded();

    for (int nTry = 0; nTry < maxTries; nTry++) {
      try {
        return delegate.read(b, off, len);
      }
      catch (Throwable t) {
        waitOrThrow(t, nTry);
      }
    }

    // Can't happen, because the final waitOrThrow would have thrown.
    throw new IllegalStateException();
  }

  @Override
  public long skip(long n) throws IOException
  {
    openIfNeeded();

    for (int nTry = 0; nTry < maxTries; nTry++) {
      try {
        return delegate.skip(n);
      }
      catch (Throwable t) {
        waitOrThrow(t, nTry);
      }
    }

    // Can't happen, because the final waitOrThrow would have thrown.
    throw new IllegalStateException();
  }

  @Override
  public int available() throws IOException
  {
    openIfNeeded();

    for (int nTry = 0; nTry < maxTries; nTry++) {
      try {
        return delegate.available();
      }
      catch (Throwable t) {
        waitOrThrow(t, nTry);
      }
    }

    // Can't happen, because the final waitOrThrow would have thrown.
    throw new IllegalStateException();
  }

  @Override
  public void close() throws IOException
  {
    if (delegate != null) {
      delegate.close();
    }
  }

  @VisibleForTesting
  void setNoWait()
  {
    this.doWait = false;
  }
}
