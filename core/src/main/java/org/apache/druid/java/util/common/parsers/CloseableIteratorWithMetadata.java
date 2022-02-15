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

import org.apache.druid.data.input.IntermediateRowParsingReader;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Like {@link CloseableIterator}, but has a metadata() method, which returns "metadata", which is effectively a Map<String, Object>
 * about the last value returned by next()
 *
 * The returned metadata is read-only and cannot be modified.
 *
 * This metadata can be used as additional information to pin-point the root cause of a parse exception.
 * So it can include information that helps with such exercise. For example, for a {@link org.apache.druid.data.input.TextReader}
 * that information can be the line number. Only per row context needs to be passed here so for kafka it could be an offset.
 * The source information is already available via {@link IntermediateRowParsingReader#source()} method and needn't be included
 */
public interface CloseableIteratorWithMetadata<T> extends CloseableIterator<T>
{
  Map<String, Object> metadata();

  /**
   * Like {@link CloseableIterator#map(Function)} but also supplies the metadata to the mapping function
   */
  default <R> CloseableIteratorWithMetadata<R> mapWithMetadata(BiFunction<T, Map<String, Object>, R> mapBiFunction)
  {
    final CloseableIteratorWithMetadata<T> delegate = this;

    return new CloseableIteratorWithMetadata<R>()
    {
      @Override
      public Map<String, Object> metadata()
      {
        return delegate.metadata();
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
        return mapBiFunction.apply(delegate.next(), delegate.metadata());
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }

  static <T> CloseableIteratorWithMetadata<T> fromCloseableIterator(CloseableIterator<T> delegate)
  {
    return new CloseableIteratorWithMetadata<T>()
    {

      @Override
      public Map<String, Object> metadata()
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
