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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.Closeable;
import java.io.IOException;

public class SequenceOperator implements Operator
{
  private static final Logger log = new Logger(SequenceOperator.class);

  private final Sequence<RowsAndColumns> child;

  public SequenceOperator(
      Sequence<RowsAndColumns> child
  )
  {
    this.child = child;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    Yielder<Signal> yielder = null;
    final Signal theSignal;
    if (continuation == null) {
      yielder = child.toYielder(Signal.GO, new YieldingAccumulator<Signal, RowsAndColumns>()
      {
        @Override
        public Signal accumulate(Signal accumulated, RowsAndColumns in)
        {
          final Signal pushSignal = receiver.push(in);
          switch (pushSignal) {
            case PAUSE:
              this.yield();
              return Signal.PAUSE;
            case GO:
              return Signal.GO;
            case STOP:
              this.yield();
              return Signal.STOP;
            default:
              throw new ISE("How can this be happening? signal[%s]", pushSignal);
          }
        }
      });
      theSignal = yielder.get();
    } else {
      try {
        final Yielder<Signal> castedYielder = (Yielder<Signal>) continuation;
        if (castedYielder.isDone()) {
          throw new ISE(
              "The yielder is done!  The previous go call should've resulted in completion instead of continuation"
          );
        }
        yielder = castedYielder.next(Signal.GO);
        theSignal = yielder.get();
      }
      catch (ClassCastException e) {
        try {
          if (yielder == null) {
            // Got the exception when casting the continuation, close the continuation and move on.
            continuation.close();
          } else {
            // Got the exception when reading the result from the continuation, close the yielder and move on.
            yielder.close();
          }
        }
        catch (IOException ex) {
          e.addSuppressed(
              new ISE("Unable to close continuation[%s] of type[%s]", continuation, continuation.getClass())
          );
        }
        throw e;
      }
    }

    switch (theSignal) {
      // We get GO from the yielder if the last push created a GO and there was nothing left in the sequence.
      // I.e. we are done
      case GO:
      case STOP:
        try {
          receiver.completed();
        }
        catch (RuntimeException e) {
          try {
            yielder.close();
          }
          catch (IOException ioException) {
            e.addSuppressed(ioException);
            throw e;
          }
        }

        try {
          yielder.close();
        }
        catch (IOException e) {
          // We got an exception when closing after we received a STOP signal and successfully called completed().
          // This means that the Receiver has already done what it needs, so instead of throw the exception and
          // potentially impact processing, we log instead and allow processing to continue.
          log.warn(e, "Exception thrown when closing yielder.  Logging and ignoring because results should be fine.");
        }
        return null;
      case PAUSE:
        return yielder;
    }
    throw new ISE("How can this happen!? signal[%s]", theSignal);
  }

}
