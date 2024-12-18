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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractPartitioningOperator implements Operator
{
  protected final List<String> partitionColumns;
  protected final Operator child;

  public AbstractPartitioningOperator(
      List<String> partitionColumns,
      Operator child
  )
  {
    this.partitionColumns = partitionColumns;
    this.child = child;
  }

  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    if (continuation != null) {
      Continuation cont = (Continuation) continuation;

      if (cont.iter != null) {
        HandleContinuationResult handleContinuationResult = handleContinuation(receiver, cont);
        if (!handleContinuationResult.needToContinueProcessing()) {
          return handleContinuationResult.getContinuation();
        }

        if (cont.subContinuation == null) {
          receiver.completed();
          return null;
        }
      }

      continuation = cont.subContinuation;
    }

    AtomicReference<Iterator<RowsAndColumns>> iterHolder = new AtomicReference<>();

    final Closeable retVal = child.goOrContinue(
        continuation,
        createReceiver(receiver, iterHolder)
    );

    if (iterHolder.get() != null || retVal != null) {
      return new Continuation(
          iterHolder.get(),
          retVal
      );
    } else {
      return null;
    }
  }

  protected abstract static class AbstractReceiver implements Receiver
  {
    protected final Receiver delegate;
    protected final AtomicReference<Iterator<RowsAndColumns>> iterHolder;
    protected final List<String> partitionColumns;

    public AbstractReceiver(
        Receiver delegate,
        AtomicReference<Iterator<RowsAndColumns>> iterHolder,
        List<String> partitionColumns
    )
    {
      this.delegate = delegate;
      this.iterHolder = iterHolder;
      this.partitionColumns = partitionColumns;
    }

    @Override
    public Signal push(RowsAndColumns rac)
    {
      if (rac == null) {
        throw DruidException.defensive("Should never get a null rac here.");
      }

      Iterator<RowsAndColumns> partitionsIter = getIteratorForRAC(rac);

      Signal keepItGoing = Signal.GO;
      while (keepItGoing == Signal.GO && partitionsIter.hasNext()) {
        final RowsAndColumns rowsAndColumns = partitionsIter.next();
        keepItGoing = pushPartition(rowsAndColumns, !partitionsIter.hasNext(), keepItGoing);
      }

      if (keepItGoing == Signal.PAUSE && partitionsIter.hasNext()) {
        iterHolder.set(partitionsIter);
        return Signal.PAUSE;
      }

      return keepItGoing;
    }

    @Override
    public void completed()
    {
      if (iterHolder.get() == null) {
        delegate.completed();
      }
    }

    protected Signal pushPartition(RowsAndColumns partition, boolean isLastPartition, Signal previousSignal)
    {
      return delegate.push(partition);
    }

    protected abstract Iterator<RowsAndColumns> getIteratorForRAC(RowsAndColumns rac);
  }

  protected abstract HandleContinuationResult handleContinuation(Receiver receiver, Continuation cont);

  protected abstract Receiver createReceiver(Receiver delegate, AtomicReference<Iterator<RowsAndColumns>> iterHolder);

  protected HandleContinuationResult handleNonGoCases(Signal signal, Iterator<RowsAndColumns> iter, Receiver receiver, Continuation cont)
  {
    switch (signal) {
      case PAUSE:
        if (iter.hasNext()) {
          return HandleContinuationResult.of(cont);
        }

        if (cont.subContinuation == null) {
          // We were finished anyway
          receiver.completed();
          return HandleContinuationResult.of(null);
        }

        return HandleContinuationResult.of(new Continuation(null, cont.subContinuation));

      case STOP:
        receiver.completed();
        try {
          cont.close();
        }
        catch (IOException e) {
          throw new RE(e, "Unable to close continuation");
        }
        return HandleContinuationResult.of(null);

      default:
        throw new RE("Unknown signal[%s]", signal);
    }
  }

  protected static class Continuation implements Closeable
  {
    Iterator<RowsAndColumns> iter;
    Closeable subContinuation;

    public Continuation(Iterator<RowsAndColumns> iter, Closeable subContinuation)
    {
      this.iter = iter;
      this.subContinuation = subContinuation;
    }

    @Override
    public void close() throws IOException
    {
      if (subContinuation != null) {
        subContinuation.close();
      }
    }
  }

  /**
   * This helper class helps us distinguish whether we need to continue processing or not.
   */
  protected static class HandleContinuationResult
  {
    private final Closeable continuation;
    private final boolean continueProcessing;

    protected static final HandleContinuationResult CONTINUE_PROCESSING = new HandleContinuationResult(null, true);

    private HandleContinuationResult(Closeable continuation, boolean continueProcessing)
    {
      this.continuation = continuation;
      this.continueProcessing = continueProcessing;
    }

    protected static HandleContinuationResult of(Closeable closeable)
    {
      return new HandleContinuationResult(closeable, false);
    }

    private boolean needToContinueProcessing()
    {
      return continueProcessing;
    }

    private Closeable getContinuation()
    {
      return continuation;
    }
  }
}
