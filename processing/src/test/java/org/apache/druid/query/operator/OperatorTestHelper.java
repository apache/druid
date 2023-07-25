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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.junit.Assert;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class OperatorTestHelper
{
  private Supplier<TestReceiver> receiverSupply;
  private Consumer<TestReceiver> finalValidation;

  public OperatorTestHelper expectRowsAndColumns(RowsAndColumnsHelper... helpers)
  {
    return withPushFn(
        () -> new JustPushMe()
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
        () -> new JustPushMe()
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

  public OperatorTestHelper withPushFn(Supplier<JustPushMe> fnSupplier)
  {
    return withReceiver(() -> new TestReceiver(fnSupplier.get()));
  }

  public void runToCompletion(Operator op)
  {
    TestReceiver receiver = this.receiverSupply.get();
    Operator.go(op, receiver);
    Assert.assertTrue(receiver.isCompleted());
    if (finalValidation != null) {
      finalValidation.accept(receiver);
    }

    for (int i = 1; i < receiver.getNumPushed(); ++i) {
      long expectedNumPauses = receiver.getNumPushed() / i;
      if (receiver.getNumPushed() % i > 0) {
        ++expectedNumPauses;
      }
      runWhilePausing(op, expectedNumPauses, i);
    }
  }

  private void runWhilePausing(Operator op, long expectedNumPauses, int pauseAfter)
  {
    // We are now going to do the same pushes and the same validation, but pausing after every possible point
    // that we could pause.  It should still produce the same results as running through without any pauses.
    TestReceiver pausingReceiver = this.receiverSupply.get();
    pausingReceiver.setPauseAfter(pauseAfter);

    int numPauses = 0;
    Closeable continuation = null;
    do {
      continuation = op.goOrContinue(continuation, pausingReceiver);
      ++numPauses;
    } while (continuation != null);

    final String msg = StringUtils.format("pauseAfter[%,d]", pauseAfter);
    Assert.assertTrue(msg, pausingReceiver.isCompleted());
    Assert.assertEquals(msg, expectedNumPauses, numPauses);
    if (finalValidation != null) {
      finalValidation.accept(pausingReceiver);
    }
  }

  public interface JustPushMe
  {
    Operator.Signal push(RowsAndColumns rac);
  }

  public static class TestReceiver implements Operator.Receiver
  {
    private final JustPushMe pushFn;

    private AtomicInteger numPushed = new AtomicInteger();
    private AtomicBoolean completed = new AtomicBoolean(false);
    private long pauseAfter = -1;

    public TestReceiver(JustPushMe pushFn)
    {
      this.pushFn = pushFn;
    }

    @Override
    public Operator.Signal push(RowsAndColumns rac)
    {
      numPushed.incrementAndGet();

      final Operator.Signal push = pushFn.push(rac);

      if (push == Operator.Signal.GO && pauseAfter != -1 && numPushed.get() % pauseAfter == 0) {
        return Operator.Signal.PAUSE;
      }

      return push;
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

    public int getNumPushed()
    {
      return numPushed.get();
    }

    public void setPauseAfter(long pauseAfter)
    {
      this.pauseAfter = pauseAfter;
    }
  }
}
