/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input.impl.prefetch;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.druid.java.util.common.RetryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;

class RetryingInputStream<T> extends InputStream
{
  private final Predicate<Throwable> retryCondition;
  private final int maxRetry;

  private InputStream delegate;
  private long readMark;

  RetryingInputStream(
      T object,
      ObjectOpenFunction<T> objectOpenFunction,
      Predicate<Throwable> retryCondition,
      int maxRetry
  ) throws IOException
  {
    this.retryCondition = t -> {
      Preconditions.checkNotNull(t);

      if (t instanceof SocketTimeoutException || t.getCause() instanceof SocketTimeoutException) {
        try {
          this.delegate.close();
          this.delegate = objectOpenFunction.open(object);

          delegate.skip(readMark);
          return true;
        }
        catch (IOException ioe) {
          t.addSuppressed(ioe);
          throw new RuntimeException(t);
        }
      }

      return retryCondition.apply(t);
    };
    this.maxRetry = maxRetry;

    this.delegate = objectOpenFunction.open(object);
  }

  private <V extends Number> Callable<V> wrap(Callable<V> fn)
  {
    return () -> {
      final V result = fn.call();
      readMark += result.longValue();
      return result;
    };
  }

  @Override
  public int read() throws IOException
  {
    try {
      return RetryUtils.retry(
          wrap(delegate::read),
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(byte b[]) throws IOException
  {
    try {
      return RetryUtils.retry(
          wrap(() -> delegate.read(b)),
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException
  {
    try {
      return RetryUtils.retry(
          wrap(() -> delegate.read(b, off, len)),
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public long skip(long n) throws IOException
  {
    try {
      return RetryUtils.retry(
          wrap(() -> delegate.skip(n)),
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int available() throws IOException
  {
    try {
      return RetryUtils.retry(
          delegate::available,
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException
  {
    try {
      RetryUtils.retry(
          () -> {
            delegate.close();
            return null;
          },
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void mark(int readlimit)
  {
    try {
      RetryUtils.retry(
          () -> {
            delegate.mark(readlimit);
            return null;
          },
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() throws IOException
  {
    try {
      RetryUtils.retry(
          () -> {
            delegate.reset();
            return null;
          },
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean markSupported()
  {
    try {
      return RetryUtils.retry(
          delegate::markSupported,
          retryCondition,
          maxRetry
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
