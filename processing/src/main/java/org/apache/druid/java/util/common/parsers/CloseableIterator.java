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

package org.apache.druid.java.util.common.parsers;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable
{
  default <R> CloseableIterator<R> map(Function<T, R> mapFunction)
  {
    final CloseableIterator<T> delegate = this;

    return new CloseableIterator<R>()
    {
      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public R next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return mapFunction.apply(delegate.next());
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }

  default <R> CloseableIterator<R> flatMap(Function<T, CloseableIterator<R>> function)
  {
    final CloseableIterator<T> outerIterator = this;

    return new CloseableIterator<R>()
    {
      CloseableIterator<R> currInnerIterator = null;

      private void findNextIteratorIfNecessary()
      {
        while ((currInnerIterator == null || !currInnerIterator.hasNext()) && outerIterator.hasNext()) {
          if (currInnerIterator != null) {
            try {
              currInnerIterator.close();
              currInnerIterator = null;
            }
            catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
          currInnerIterator = function.apply(outerIterator.next());
          if (currInnerIterator.hasNext()) {
            return;
          }
        }
      }

      @Override
      public boolean hasNext()
      {
        // closes the current iterator if it is finished, and opens a new non-empty iterator if possible
        findNextIteratorIfNecessary();
        return currInnerIterator != null && currInnerIterator.hasNext();
      }

      @Override
      public R next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return currInnerIterator.next();
      }

      @Override
      public void close() throws IOException
      {
        outerIterator.close();
        if (currInnerIterator != null) {
          currInnerIterator.close();
          currInnerIterator = null;
        }
      }
    };
  }
}
