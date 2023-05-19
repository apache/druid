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

package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Provides a sequence on top of Operators.
 */
public class OperatorSequence implements Sequence<RowsAndColumns>
{
  private final Supplier<Operator> opSupplier;

  public OperatorSequence(
      Supplier<Operator> opSupplier
  )
  {
    this.opSupplier = opSupplier;
  }

  @Override
  public <OutType> OutType accumulate(
      final OutType initValue,
      Accumulator<OutType, RowsAndColumns> accumulator
  )
  {
    final MyReceiver<OutType> receiver = new MyReceiver<>(
        initValue,
        new YieldingAccumulator<OutType, RowsAndColumns>()
        {
          @Override
          public OutType accumulate(OutType accumulated, RowsAndColumns in)
          {
            return accumulator.accumulate(accumulated, in);
          }
        }
    );
    Operator.go(opSupplier.get(), receiver);
    return receiver.getRetVal();
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue,
      YieldingAccumulator<OutType, RowsAndColumns> accumulator
  )
  {
    final Operator op = opSupplier.get();
    final MyReceiver<OutType> receiver = new MyReceiver<>(initValue, accumulator);
    final Closeable finalContinuation = op.goOrContinue(null, receiver);
    if (finalContinuation == null && !accumulator.yielded()) {
      // We finished processing, and the accumulator did not yield, so we return a done yielder with our value
      return Yielders.done(receiver.getRetVal(), null);
    } else {
      return new Yielder<OutType>()
      {
        private Closeable continuation = finalContinuation;

        @Override
        public OutType get()
        {
          return receiver.getRetVal();
        }

        @Override
        public Yielder<OutType> next(OutType initValue)
        {
          if (continuation == null) {
            // This means that we completed processing on the previous run.  In this case, we are all done
            return Yielders.done(null, null);
          }
          receiver.setRetVal(initValue);

          continuation = op.goOrContinue(continuation, receiver);
          return this;
        }

        @Override
        public boolean isDone()
        {
          return false;
        }

        @Override
        public void close() throws IOException
        {
          if (continuation != null) {
            continuation.close();
          }
        }
      };
    }
  }

  private static class MyReceiver<OutType> implements Operator.Receiver
  {
    private final YieldingAccumulator<OutType, RowsAndColumns> accumulator;
    private OutType retVal;

    public MyReceiver(OutType initValue, YieldingAccumulator<OutType, RowsAndColumns> accumulator)
    {
      this.accumulator = accumulator;
      retVal = initValue;
    }

    public void setRetVal(OutType retVal)
    {
      this.retVal = retVal;
    }

    public OutType getRetVal()
    {
      return retVal;
    }

    @Override
    public Operator.Signal push(RowsAndColumns rac)
    {
      retVal = accumulator.accumulate(retVal, rac);
      return accumulator.yielded() ? Operator.Signal.PAUSE : Operator.Signal.GO;
    }

    @Override
    public void completed()
    {

    }
  }
}
