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
import com.google.common.io.CountingInputStream;
import io.druid.java.util.common.RetryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;

class RetryingInputStream<T> extends InputStream
{
  private final Predicate<Throwable> retryCondition;
  private final int maxTry;

  private CountingInputStream delegate;

  RetryingInputStream(
      T object,
      ObjectOpenFunction<T> objectOpenFunction,
      Predicate<Throwable> retryCondition,
      int maxTry
  ) throws IOException
  {
    this.retryCondition = t -> {
      Preconditions.checkNotNull(t);

      if (t instanceof SocketTimeoutException || t.getCause() instanceof SocketTimeoutException) {
        try {
          final long count = delegate.getCount();
          delegate.close();
          delegate = new CountingInputStream(objectOpenFunction.open(object, count));
          return true;
        }
        catch (IOException ioe) {
          t.addSuppressed(ioe);
          throw new RuntimeException(t);
        }
      }

      return retryCondition.apply(t);
    };

    this.maxTry = maxTry + 1;
    this.delegate = new CountingInputStream(objectOpenFunction.open(object));
  }

  @Override
  public int read() throws IOException
  {
    try {
      return RetryUtils.retry(
          delegate::read,
          retryCondition,
          maxTry
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
          () -> delegate.read(b),
          retryCondition,
          maxTry
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
          () -> delegate.read(b, off, len),
          retryCondition,
          maxTry
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
          () -> delegate.skip(n),
          retryCondition,
          maxTry
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
          maxTry
      );
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}
