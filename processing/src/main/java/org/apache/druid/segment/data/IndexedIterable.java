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

package org.apache.druid.segment.data;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class IndexedIterable<T> implements Iterable<T>
{
  private final Indexed<T> indexed;

  public static <T> IndexedIterable<T> create(Indexed<T> indexed)
  {
    return new IndexedIterable<T>(indexed);
  }

  public IndexedIterable(Indexed<T> indexed)
  {
    this.indexed = indexed;
  }

  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<T>()
    {
      private int currIndex = 0;

      @Override
      public boolean hasNext()
      {
        return currIndex < indexed.size();
      }

      @Override
      public T next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return indexed.get(currIndex++);
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
