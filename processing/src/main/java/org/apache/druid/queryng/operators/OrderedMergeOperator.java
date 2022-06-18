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

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.PriorityQueue;
import java.util.function.Supplier;

/**
 * Perform an in-memory, n-way merge on n ordered inputs.
 * Ordering is given by an {@link Ordering} which defines
 * the set of fields to order by, and their comparison
 * functions.
 *
 * @see {@link org.apache.druid.query.RetryQueryRunner}
 */
public class OrderedMergeOperator<T> implements IterableOperator<T>
{
  /**
   * Manages a single input characterized by the operator for that
   * input, an iterator over that operator, and the current value
   * for that iterator. This class caches the current value so that
   * the priority queue can perform comparisons on it.
   */
  public static class Input<T>
  {
    private final Operator<T> input;
    private final ResultIterator<T> iter;
    private T currentValue;

    public Input(Operator<T> input)
    {
      this.input = input;
      this.iter = input.open();
      try {
        currentValue = iter.next();
      }
      catch (EofException e) {
        currentValue = null;
        input.close(true);
      }
    }

    public Operator<T> toOperator(FragmentContext context)
    {
      if (currentValue == null) {
        return new NullOperator<T>(context);
      } else {
        return new PushBackOperator<T>(context, input, iter, currentValue);
      }
    }

    public T get()
    {
      return currentValue;
    }

    public boolean next()
    {
      try {
        currentValue = iter.next();
        return true;
      }
      catch (EofException e) {
        currentValue = null;
        input.close(true);
        return false;
      }
    }

    public boolean eof()
    {
      return currentValue == null;
    }

    public void close(boolean cascade)
    {
      if (currentValue != null) {
        currentValue = null;
        input.close(cascade);
      }
    }
  }

  /**
   * Supplier of a collection (iterable) of inputs as defined by an
   * operator, iterator and current item. To unpack this a bit, the input
   * to the merge is the mechanism which distributes fragments across
   * multiple threads, waits for the response, checks for missing fragments
   * and retries. All this is opaque to this class which just wants the
   * final collection that should be merged. That collection is wrapped
   * in a supplier so that it is not started until {@link #open()} time.
   */
  private final Supplier<Iterable<Input<T>>> inputs;
  private final PriorityQueue<Input<T>> pQueue;

  public OrderedMergeOperator(
      FragmentContext context,
      Ordering<? super T> ordering,
      int approxInputCount,
      Supplier<Iterable<Input<T>>> inputs
  )
  {
    this.inputs = inputs;
    this.pQueue = new PriorityQueue<>(
        approxInputCount == 0 ? 1 : approxInputCount,
        ordering.onResultOf(input -> input.get())
    );
    context.register(this);
  }

  @Override
  public ResultIterator<T> open()
  {
    for (Input<T> input : inputs.get()) {
      if (!input.eof()) {
        pQueue.add(input);
      }
    }
    return this;
  }

  @Override
  public T next() throws EofException
  {
    if (pQueue.isEmpty()) {
      throw Operators.eof();
    }
    Input<T> input = pQueue.remove();
    T result = input.get();
    if (input.next()) {
      pQueue.add(input);
    }
    return result;
  }

  @Override
  public void close(boolean cascade)
  {
    while (!pQueue.isEmpty()) {
      Input<T> input = pQueue.remove();
      input.close(cascade);
    }
  }
}
