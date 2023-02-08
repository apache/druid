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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class OperatorTestHelper
{
  private Supplier<TestReceiver> receiverSupply;
  private Consumer<TestReceiver> finalValidation;

  public OperatorTestHelper expectRowsAndColumns(RowsAndColumnsHelper... helpers)
  {
    return withPushFn(
        new JustPushMe()
        {
          int index = 0;

          @Override
          public Operator.Signal push(RowsAndColumns rac)
          {
            helpers[index++].validate(rac);
            return Operator.Signal.GO;
          }
        }
    ).withFinalValidation(
        testReceiver -> Assert.assertEquals(helpers.length, testReceiver.getNumPushed())
    );
  }

  public OperatorTestHelper expectAndStopAfter(RowsAndColumnsHelper... helpers)
  {
    return withPushFn(
        new JustPushMe()
        {
          int index = 0;

          @Override
          public Operator.Signal push(RowsAndColumns rac)
          {
            helpers[index++].validate(rac);
            return index < helpers.length ? Operator.Signal.GO : Operator.Signal.STOP;
          }
        }
    ).withFinalValidation(
        testReceiver -> Assert.assertEquals(helpers.length, testReceiver.getNumPushed())
    );
  }

  public OperatorTestHelper withReceiver(Supplier<TestReceiver> receiver)
  {
    if (this.receiverSupply != null) {
      throw new ISE("Receiver[%s] already set, cannot set it again[%s].", this.receiverSupply, receiver);
    }
    this.receiverSupply = receiver;
    return this;
  }

  public OperatorTestHelper withFinalValidation(Consumer<TestReceiver> validator)
  {
    if (finalValidation == null) {
      this.finalValidation = validator;
    } else {
      final Consumer<TestReceiver> subValidator = finalValidation;
      this.finalValidation = (receiver) -> {
        subValidator.accept(receiver);
        validator.accept(receiver);
      };
    }
    return this;
  }

  public OperatorTestHelper withPushFn(JustPushMe fn)
  {
    return withReceiver(() -> new TestReceiver(fn));
  }

  public OperatorTestHelper runToCompletion(Operator op)
  {
    TestReceiver receiver = this.receiverSupply.get();
    Operator.go(op, receiver);
    Assert.assertTrue(receiver.isCompleted());
    if (finalValidation != null) {
      finalValidation.accept(receiver);
    }
    return this;
  }

  public interface JustPushMe
  {
    Operator.Signal push(RowsAndColumns rac);
  }

  public static class TestReceiver implements Operator.Receiver
  {
    private final JustPushMe pushFn;

    private AtomicLong numPushed = new AtomicLong();
    private AtomicBoolean completed = new AtomicBoolean(false);

    public TestReceiver(JustPushMe pushFn)
    {
      this.pushFn = pushFn;
    }

    @Override
    public Operator.Signal push(RowsAndColumns rac)
    {
      numPushed.incrementAndGet();
      return pushFn.push(rac);
    }

    public boolean isCompleted()
    {
      return completed.get();
    }

    @Override
    public void completed()
    {
      if (!completed.compareAndSet(false, true)) {
        throw new ISE("complete called more than once!?  Why.");
      }
    }

    public long getNumPushed()
    {
      return numPushed.get();
    }
  }
}
