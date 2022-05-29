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
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.general.QueryRunnerOperator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Utility functions related to operators.
 */
public class Operators
{
  /**
   * Convenience function to open the operator and return its
   * iterator as an {@code Iterable}.
   */
  public static <T> Iterable<T> toIterable(Operator<T> op)
  {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator()
      {
        return new Iterators.ShimIterator<T>(op.open());
      }
    };
  }

  /**
   * Wraps an operator in a sequence using the standard base sequence
   * iterator mechanism (since an operator looks like an iterator.)
   *
   * This is a named class so we can unwrap the operator in
   * {@link static <T> Operator<T> #toOperator(DAGBuilder, Sequence<T>)}.
   */
  public static class OperatorWrapperSequence<T> extends BaseSequence<T, Iterator<T>>
  {
    private final Operator<T> op;

    public OperatorWrapperSequence(Operator<T> op)
    {
      super(new BaseSequence.IteratorMaker<T, Iterator<T>>()
      {
        @Override
        public Iterator<T> make()
        {
          return toIterator(op);
        }

        @Override
        public void cleanup(Iterator<T> iterFromMake)
        {
          op.close(true);
        }
      });
      this.op = op;
    }

    public Operator<T> unwrap()
    {
      return op;
    }

    /**
     * This will materialize the entire sequence from the wrapped
     * operator.  Use at your own risk.
     */
    @Override
    public List<T> toList()
    {
      return Operators.toList(op);
    }
  }

  /**
   * Converts a stand-alone operator to a sequence outside the context of a fragment
   * runner. The sequence starts and closes the operator.
   */
  public static <T> Sequence<T> toSequence(Operator<T> op)
  {
    return new OperatorWrapperSequence<>(op);
  }

  /**
   * Wrap a sequence in an operator.
   * <p>
   * If the input sequence is a wrapper around an operator, then
   * (clumsily) unwraps that operator and returns it directly.
   */
  public static <T> Operator<T> toOperator(FragmentContext context, Sequence<T> sequence)
  {
    if (sequence instanceof OperatorWrapperSequence) {
      return ((OperatorWrapperSequence<T>) sequence).unwrap();
    }
    return new SequenceOperator<T>(context, sequence);
  }

  public static <T> Operator<T> unwrapOperator(Sequence<T> sequence)
  {
    if (sequence instanceof OperatorWrapperSequence) {
      return ((OperatorWrapperSequence<T>) sequence).unwrap();
    }
    return null;
  }

  /**
   * Create an operator which wraps a query runner which allows a query runner
   * to be an input to an operator. The runner, and its sequence, will be optimized
   * away at runtime if both the upstream and downstream items are both operators,
   * but the shim is left in place if the upstream is actually a query runner.
   */
  public static <T> QueryRunnerOperator<T> toOperator(QueryRunner<T> runner, QueryPlus<T> query)
  {
    return new QueryRunnerOperator<T>(runner, query);
  }

  public static <T> Iterator<T> toIterator(Operator<T> op)
  {
    return new Iterators.ShimIterator<T>(op.open());
  }

  /**
   * This will materialize the entire sequence from the wrapped
   * operator.  Use at your own risk.
   */
  public static <T> List<T> toList(Operator<T> op)
  {
    List<T> results = Lists.newArrayList(toIterator(op));
    op.close(true);
    return results;
  }

  public static ResultIterator.EofException eof()
  {
    return new ResultIterator.EofException();
  }

  public static void closeSafely(Closer closer)
  {
    try {
      closer.close();
    }

    // What the method claims to throw
    catch (IOException e) {
      throw new RE(e, "Failed to close");
    }

    // What can be thrown invisibly, no need to wrap.
    catch (RuntimeException e) {
      throw e;
    }

    // What existing code checks for
    catch (Throwable t) {
      throw new RE(t, "Failed to close");
    }
  }
}
