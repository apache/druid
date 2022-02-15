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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Like {@link CloseableIterator}, but has a parseContext() method, which returns a Map<String, Object> about the current
 * value, or null if the current value is null
 * <p>
 * The returned context map is readonly and cannot be modified
 */
public interface CloseableIteratorWithParseContext<T> extends CloseableIterator<T>
{
  Map<String, Object> parseContext();

  default <R> CloseableIteratorWithParseContext<R> mapWithParseContext(BiFunction<T, Map<String, Object>, R> mapBiFunction)
  {
    final CloseableIteratorWithParseContext<T> delegate = this;

    return new CloseableIteratorWithParseContext<R>()
    {
      @Override
      public Map<String, Object> parseContext()
      {
        return delegate.parseContext();
      }

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
        return mapBiFunction.apply(delegate.next(), delegate.parseContext());
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }

  static <T> CloseableIteratorWithParseContext<T> fromCloseableIterator(CloseableIterator<T> delegate)
  {
    return new CloseableIteratorWithParseContext<T>()
    {

      @Override
      public Map<String, Object> parseContext()
      {
        return Collections.emptyMap();
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }

      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public T next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return delegate.next();
      }
    };
  }
}
