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

package io.druid.query.groupby.epinephelinae;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class CloseableGrouperIterator<KeyType extends Comparable<KeyType>, T> implements Iterator<T>, Closeable
{
  private final Function<Grouper.Entry<KeyType>, T> transformer;
  private final Closeable closer;
  private final Iterator<Grouper.Entry<KeyType>> iterator;

  public CloseableGrouperIterator(
      final Grouper<KeyType> grouper,
      final boolean sorted,
      final Function<Grouper.Entry<KeyType>, T> transformer,
      final Closeable closer
  )
  {
    this.transformer = transformer;
    this.closer = closer;
    this.iterator = grouper.iterator(sorted);
  }

  @Override
  public T next()
  {
    return transformer.apply(iterator.next());
  }

  @Override
  public boolean hasNext()
  {
    return iterator.hasNext();
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {
    if (closer != null) {
      try {
        closer.close();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
