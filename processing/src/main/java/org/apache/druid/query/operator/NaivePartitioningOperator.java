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
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ClusteredGroupPartitioner;
import org.apache.druid.query.rowsandcols.semantic.DefaultClusteredGroupPartitioner;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This naive partitioning operator assumes that it's child operator always gives it RowsAndColumns objects that are
 * a superset of the partitions that it needs to provide.  It will never attempt to make a partition larger than a
 * single RowsAndColumns object that it is given from its child Operator.  A different operator should be used
 * if that is an important bit of functionality to have.
 * <p>
 * Additionally, this assumes that data has been pre-sorted according to the partitioning columns.  If it is
 * given data that has not been pre-sorted, an exception is expected to be thrown.
 */
public class NaivePartitioningOperator extends AbstractPartitioningOperator
{
  public NaivePartitioningOperator(
      List<String> partitionColumns,
      Operator child
  )
  {
    super(partitionColumns, child);
  }

  @Override
  protected HandleContinuationResult handleContinuation(Receiver receiver, Continuation cont)
  {
    while (cont.iter.hasNext()) {
      final Signal signal = receiver.push(cont.iter.next());
      if (signal != Signal.GO) {
        return handleNonGoCases(signal, cont.iter, receiver, cont);
      }
    }
    return HandleContinuationResult.CONTINUE_PROCESSING;
  }

  private static class StaticReceiver implements Receiver
  {
    Receiver delegate;
    AtomicReference<Iterator<RowsAndColumns>> iterHolder;
    List<String> partitionColumns;

    public StaticReceiver(Receiver delegate, AtomicReference<Iterator<RowsAndColumns>> iterHolder, List<String> partitionColumns)
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

      Iterator<RowsAndColumns> partitionsIter = getIteratorForRAC(rac, partitionColumns);

      AtomicReference<Signal> keepItGoing = new AtomicReference<>(Signal.GO);
      while (keepItGoing.get() == Signal.GO && partitionsIter.hasNext()) {
        handleKeepItGoing(keepItGoing, partitionsIter, delegate);
      }

      if (keepItGoing.get() == Signal.PAUSE && partitionsIter.hasNext()) {
        iterHolder.set(partitionsIter);
        return Signal.PAUSE;
      }

      return keepItGoing.get();
    }

    @Override
    public void completed()
    {
      if (iterHolder.get() == null) {
        delegate.completed();
      }
    }
  }

  protected static Iterator<RowsAndColumns> getIteratorForRAC(RowsAndColumns rac, List<String> partitionColumns)
  {
    ClusteredGroupPartitioner groupPartitioner = rac.as(ClusteredGroupPartitioner.class);
    if (groupPartitioner == null) {
      groupPartitioner = new DefaultClusteredGroupPartitioner(rac);
    }
    return groupPartitioner.partitionOnBoundaries(partitionColumns).iterator();
  }

  protected static void handleKeepItGoing(AtomicReference<Signal> signalRef, Iterator<RowsAndColumns> iterator, Receiver receiver)
  {
    RowsAndColumns next = iterator.next();
    signalRef.set(receiver.push(next));
  }

  @Override
  protected Receiver createReceiver(Receiver delegate, AtomicReference<Iterator<RowsAndColumns>> iterHolder)
  {
    return new StaticReceiver(delegate, iterHolder, partitionColumns);
  }
}
