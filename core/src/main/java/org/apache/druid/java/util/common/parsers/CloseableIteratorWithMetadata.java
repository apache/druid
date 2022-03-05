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

/**
 * Like {@link CloseableIterator}, but has a currentMetadata() method, which returns "metadata", which is effectively a Map<String, Object>
 * about the source of last value returned by next()
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

  /**
   * @return A map containing the information about the source of the last value returned by {@link #next()}
   */
  Map<String, Object> currentMetadata();

  /**
   * Creates an instance of CloseableIteratorWithMetadata from a {@link CloseableIterator}. {@link #currentMetadata()}
   * for the instance is guaranteed to return an empty map
   */
  static <T> CloseableIteratorWithMetadata<T> withEmptyMetadata(CloseableIterator<T> delegate)
  {
    return new CloseableIteratorWithMetadata<T>()
    {

      @Override
      public Map<String, Object> currentMetadata()
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
        return delegate.next();
      }
    };
  }
}
