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

import com.google.common.base.Suppliers;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;


public class SummaryRowSupplierVectorGrouper implements VectorGrouper
{
  private final VectorGrouper delegate;
  private final AggregatorAdapters aggregatorAdapters;
  private ByteBuffer buffer;

  public SummaryRowSupplierVectorGrouper(VectorGrouper grouper, AggregatorAdapters aggregatorAdapters)
  {
    delegate = grouper;
    this.aggregatorAdapters = aggregatorAdapters;
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
    buffer = ByteBuffer.allocate(1 + 2 * aggregatorAdapters.spaceNeeded());
    BufferArrayGrouper bag = new BufferArrayGrouper(Suppliers.ofInstance(buffer), aggregatorAdapters, 1)
    {
      public void init()
      {
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
