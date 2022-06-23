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

package org.apache.druid.queryng.operators;

import com.google.common.collect.Lists;
import org.apache.druid.queryng.operators.Operator.EofException;
import org.apache.druid.queryng.operators.Operator.ResultIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility methods on top of {@link Operator.ResultIterator RowIterator},
 * including conversion to a Java iterator (primarily for testing.)
 */
public class Iterators
{
  public static class ShimIterator<T> implements Iterator<T>
  {
    private final ResultIterator<T> operIter;
    private boolean eof;
    private T lookAhead;

    public ShimIterator(ResultIterator<T> operIter)
    {
      this.operIter = operIter;
    }

    @Override
    public boolean hasNext()
    {
      if (eof) {
        return false;
      }
      try {
        lookAhead = operIter.next();
        return true;
      }
      catch (EofException e) {
        eof = false;
        return false;
      }
    }

    @Override
    public T next()
    {
      if (eof || lookAhead == null) {
        throw new NoSuchElementException();
      }
      return lookAhead;
    }

  }

  public static <T> Iterable<T> toIterable(ResultIterator<T> iter)
  {
    return Iterators.toIterable(Iterators.toIterator(iter));
  }

  public static <T> Iterable<T> toIterable(Iterator<T> iter)
  {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator()
      {
        return iter;
      }
    };
  }

  public static <T> Iterator<T> toIterator(ResultIterator<T> opIter)
  {
    return new Iterators.ShimIterator<T>(opIter);
  }

  public static <T> List<T> toList(ResultIterator<T> operIter)
  {
    return Lists.newArrayList(new Iterators.ShimIterator<T>(operIter));
  }

  public static <T> ResultIterator<T> emptyIterator()
  {
    return new ResultIterator<T>()
    {
      @Override
      public T next() throws EofException
      {
        throw Operators.eof();
      }
    };
  }

}
