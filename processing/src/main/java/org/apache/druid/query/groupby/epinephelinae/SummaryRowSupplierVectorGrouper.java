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

package org.apache.druid.query.groupby.epinephelinae;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import com.google.common.base.Supplier;

public class SummaryRowSupplierVectorGrouper implements VectorGrouper
{
  private VectorGrouper delegate;
  private List<AggregatorFactory> aggregatorSpecs;
  private VectorColumnSelectorFactory columnSelectorFactory;
  private Supplier<ByteBuffer> supplier;

  public SummaryRowSupplierVectorGrouper(VectorGrouper grouper, Supplier<ByteBuffer> supplier, List<AggregatorFactory> aggregatorSpecs,
      VectorColumnSelectorFactory columnSelectorFactory)
  {
    delegate = grouper;
    this.supplier = supplier;
    this.aggregatorSpecs = aggregatorSpecs;
    this.columnSelectorFactory = columnSelectorFactory;

  }

  @Override
  public void initVectorized(int maxVectorSize)
  {
    delegate.initVectorized(maxVectorSize);
  }

  @Override
  public AggregateResult aggregateVector(Memory keySpace, int startRow, int endRow)
  {
    return delegate.aggregateVector(keySpace, startRow, endRow);
  }

  @Override
  public void reset()
  {
    delegate.reset();
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  private CloseableIterator<Entry<MemoryPointer>> buildSummaryRow()
  {
//    final MemoryPointer reusableKey = new MemoryPointer();
//    final ReusableEntry<MemoryPointer> reusableEntry = new ReusableEntry<>(reusableKey, new Object[aggregators.size()]);
//    // final ReusableEntry<Entry<MemoryPointer>> reusableEntry =
//    // ReusableEntry.create(keySerde, aggregatorFactories.length);
//    Object[] values = reusableEntry.getValues();
//
AggregatorAdapters ada = AggregatorAdapters.factorizeVector(
        columnSelectorFactory,
        aggregatorSpecs
    );
//
//
//    for (int i = 0; i < aggregatorSpecs.size(); i++) {
//
//      ada.init(null, i);
//      ada.get
//
//      aggregatorSpecs.get(i).factorize(columnSelectorFactory);
//      aggregatorAdapters.Aggregator aggregate = aggregatorFactories[i].factorize(columnSelectorFactory);
//      values[i] = aggregate.get();
//    }
//    return reusableEntry;
    BufferArrayGrouper bag = new BufferArrayGrouper(supplier, ada, 0) {
      public void init() {
        super.init();
        initializeSlotIfNeeded(0);
      }
    };
    bag.init();
    return bag.iterator();

  }

  @Override
  public CloseableIterator<Entry<MemoryPointer>> iterator()
  {
    CloseableIterator<Entry<MemoryPointer>> it = delegate.iterator();
    return new CloseableIterator<Entry<MemoryPointer>>()
    {
      boolean delegated;
      boolean done;

      @Override
      public boolean hasNext()
      {
        if (it.hasNext()) {
          delegated = true;
          return true;
        }
        if (delegated) {
          return it.hasNext();
        }
        return !done;
      }

      @Override
      public Entry<MemoryPointer> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        if (delegated) {
          return it.next();
        }
        done = true;
        return buildSummaryRow().next();
      }

      @Override
      public void close() throws IOException
      {
        it.close();
      }
    };

  }
}
