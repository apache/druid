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
import org.apache.druid.java.util.common.guava.Yielders;
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
    Yielder<RowsAndColumns> yielder = null;
    try {
      if (continuation == null) {
        yielder = Yielders.each(child);
      } else {
        yielder = (Yielder<RowsAndColumns>) continuation;
      }

      while (true) {
        Signal signal;
        if (yielder.isDone()) {
          if (yielder.get() != null) {
            throw new ISE("Got a non-null get()[%s] even though we were already done.", yielder.get().getClass());
          }
          signal = Signal.STOP;
        } else {
          signal = receiver.push(yielder.get());
        }

        if (signal != Signal.STOP) {
          yielder = yielder.next(null);
          if (yielder.isDone()) {
            signal = Signal.STOP;
          }
        }

        switch (signal) {
          case STOP:
            receiver.completed();

            try {
              yielder.close();
            }
            catch (IOException e) {
              // We got an exception when closing after we received a STOP signal and successfully called completed().
              // This means that the Receiver has already done what it needs, so instead of throw the exception and
              // potentially impact processing, we log instead and allow processing to continue.
              log.warn(
                  e,
                  "Exception thrown when closing yielder.  Logging and ignoring because results should be fine."
              );
            }
            return null;
          case PAUSE:
            return yielder;
          case GO:
            continue;
          default:
            throw new ISE("Unknown signal[%s]", signal);
        }
      }
    }
    catch (RuntimeException re) {
      try {
        if (yielder != null) {
          yielder.close();
        } else if (continuation != null) {
          // The yielder will be non-null in most cases, this can likely only happen if the continuation that we
          // received was not able to be cast to a yielder.
          continuation.close();
        }
      }
      catch (IOException ioEx) {
        re.addSuppressed(ioEx);
      }
      throw re;
    }
  }
}
