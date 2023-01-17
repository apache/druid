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
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.util.function.Supplier;

/**
 * Provides a sequence on top of Operators.  The mis-match in pull (Sequence) and push (Operator) means that, if we
 * choose to support the Yielder interface, we have to use threading.  Managing extra threads in order to do that
 * is unfortunate, so, we choose to take a bit of a cop-out approach.
 *
 * Specifically, the accumulate method doesn't actually have the same problem and the query pipeline after the merge
 * functions is composed of Sequences that all use accumulate instead of yielder.  Thus, if we are certain that
 * we only use the OperatorSequence in places where toYielder is not called (i.e. it's only used as the return
 * value of the merge() calls), then we can get away with only implementing the accumulate path.
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
    final MyReceiver<OutType> receiver = new MyReceiver<>(initValue, accumulator);
    opSupplier.get().go(receiver);
    return receiver.getRetVal();
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue,
      YieldingAccumulator<OutType, RowsAndColumns> accumulator
  )
  {
    // As mentioned in the class-level javadoc, we skip this implementation and leave it up to the developer to
    // only use this class in "safe" locations.
    throw new UnsupportedOperationException("Cannot convert an Operator to a Yielder");
  }

  private static class MyReceiver<OutType> implements Operator.Receiver
  {
    private final Accumulator<OutType, RowsAndColumns> accumulator;
    private OutType retVal;

    public MyReceiver(OutType initValue, Accumulator<OutType, RowsAndColumns> accumulator)
    {
      this.accumulator = accumulator;
      retVal = initValue;
    }

    public OutType getRetVal()
    {
      return retVal;
    }

    @Override
    public boolean push(RowsAndColumns rac)
    {
      retVal = accumulator.accumulate(retVal, rac);
      return true;
    }

    @Override
    public void completed()
    {

    }
  }
}
