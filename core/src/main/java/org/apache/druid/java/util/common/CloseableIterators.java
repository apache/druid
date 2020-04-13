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

package org.apache.druid.java.util.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class CloseableIterators
{
  public static <T> CloseableIterator<T> concat(List<? extends CloseableIterator<? extends T>> iterators)
  {
    final Closer closer = Closer.create();
    iterators.forEach(closer::register);

    final Iterator<T> innerIterator = Iterators.concat(iterators.iterator());
    return wrap(innerIterator, closer);
  }

  public static <T> CloseableIterator<T> mergeSorted(
      List<? extends CloseableIterator<? extends T>> iterators,
      Comparator<T> comparator
  )
  {
    Preconditions.checkNotNull(comparator);

    final Closer closer = Closer.create();
    iterators.forEach(closer::register);

    final Iterator<T> innerIterator = Iterators.mergeSorted(iterators, comparator);
    return wrap(innerIterator, closer);
  }

  public static <T> CloseableIterator<T> wrap(Iterator<T> innerIterator, @Nullable Closeable closeable)
  {
    return new CloseableIterator<T>()
    {
      private boolean closed;

      @Override
      public boolean hasNext()
      {
        if (closed) {
          return false;
        }
        return innerIterator.hasNext();
      }

      @Override
      public T next()
      {
        return innerIterator.next();
      }

      @Override
      public void close() throws IOException
      {
        if (!closed) {
          if (closeable != null) {
            closeable.close();
          }
          closed = true;
        }
      }
    };
  }

  public static <T> CloseableIterator<T> withEmptyBaggage(Iterator<T> innerIterator)
  {
    return wrap(innerIterator, null);
  }

  private CloseableIterators()
  {
  }
}
