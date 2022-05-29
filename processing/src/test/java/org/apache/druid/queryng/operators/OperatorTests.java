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

import org.apache.druid.java.util.common.RE;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import static org.junit.Assert.fail;

public class OperatorTests
{
  public static void assertEof(ResultIterator<?> operIter)
  {
    try {
      operIter.next();
      fail();
    }
    catch (ResultIterator.EofException e) {
      // Expected
    }
  }

  public static <T> Operator<T> sleepOperator(
      FragmentContext context,
      Operator<T> in,
      long sleepMs
  )
  {
    return new TransformOperator<T, T>(
        context,
        in,
        row -> {
          try {
            Thread.sleep(sleepMs);
          }
          catch (InterruptedException e) {
            // Ignore
          }
          return row;
        },
        "test"
    );
  }

  /**
   * Operator to insert a failure after the nth result. Clearly only for testing.
   */
  public static class FailOperator<T> implements IterableOperator<T>
  {
    private final Operator<T> inputOp;
    private final int failAt;
    private int batchCount;
    private ResultIterator<T> inputIter;

    public FailOperator(FragmentContext context, Operator<T> input, int failAt)
    {
      this.inputOp = input;
      this.failAt = failAt;
      context.register(this);
    }

    @Override
    public ResultIterator<T> open()
    {
      inputIter = inputOp.open();
      return this;
    }

    @Override
    public T next() throws ResultIterator.EofException
    {
      if (batchCount == failAt) {
        throw new RE("Failed");
      }
      T row = inputIter.next();
      batchCount++;
      return row;
    }

    @Override
    public void close(boolean cascade)
    {
      if (cascade && inputIter != null) {
        inputOp.close(cascade);
      }
      inputIter = null;
    }
  }

  /**
   * Operator to insert a wait between results for testing.
   */
  public static <T> Operator<T> failOperator(
      final FragmentContext context,
      final Operator<T> in,
      final int onRow
  )
  {
    return new FailOperator<T>(context, in, onRow);
  }
}
