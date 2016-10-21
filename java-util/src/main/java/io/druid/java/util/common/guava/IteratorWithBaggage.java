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

package io.druid.java.util.common.guava;

import io.druid.java.util.common.parsers.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 */
public class IteratorWithBaggage<T> implements CloseableIterator<T>
{
  private final Iterator<T> baseIter;
  private final Closeable baggage;

  public IteratorWithBaggage(
      Iterator<T> baseIter,
      Closeable baggage
  )
  {
    this.baseIter = baseIter;
    this.baggage = baggage;
  }

  @Override
  public boolean hasNext()
  {
    return baseIter.hasNext();
  }

  @Override
  public T next()
  {
    return baseIter.next();
  }

  @Override
  public void remove()
  {
    baseIter.remove();
  }

  @Override
  public void close() throws IOException
  {
    baggage.close();
  }
}
