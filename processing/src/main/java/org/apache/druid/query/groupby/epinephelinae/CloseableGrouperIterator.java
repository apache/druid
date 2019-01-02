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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

public class CloseableGrouperIterator<KeyType, T> implements CloseableIterator<T>
{
  private final Function<Entry<KeyType>, T> transformer;
  private final CloseableIterator<Entry<KeyType>> iterator;
  private final Closer closer;

  public CloseableGrouperIterator(
      final CloseableIterator<Entry<KeyType>> iterator,
      final Function<Grouper.Entry<KeyType>, T> transformer,
      final Closeable closeable
  )
  {
    this.transformer = transformer;
    this.iterator = iterator;
    this.closer = Closer.create();

    closer.register(iterator);
    closer.register(closeable);
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
    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
