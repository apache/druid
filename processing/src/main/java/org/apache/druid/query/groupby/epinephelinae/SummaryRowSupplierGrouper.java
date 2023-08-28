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

import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

public class SummaryRowSupplierGrouper<KeyType> implements Grouper<KeyType>
{
  private Grouper<KeyType> delegate;
  private KeySerde<KeyType> keySerde;
  private AggregatorFactory[] aggregatorFactories;
  private ColumnSelectorFactory columnSelectorFactory;

  public SummaryRowSupplierGrouper(Grouper<KeyType> grouper, KeySerde<KeyType> keySerde,
      ColumnSelectorFactory columnSelectorFactory, List<AggregatorFactory> aggregatorFactories)
  {
    this(grouper, keySerde, columnSelectorFactory,
        aggregatorFactories.toArray(new AggregatorFactory[0]));
  }

  public SummaryRowSupplierGrouper(Grouper<KeyType> grouper, KeySerde<KeyType> keySerde,
      ColumnSelectorFactory columnSelectorFactory, AggregatorFactory[] aggregatorFactories)
  {
    delegate = grouper;
    this.keySerde = keySerde;
    this.columnSelectorFactory = columnSelectorFactory;
    this.aggregatorFactories = aggregatorFactories;
  }

  @Override
  public void init()
  {
    delegate.init();
  }

  @Override
  public boolean isInitialized()
  {
    return delegate.isInitialized();
  }

  @Override
  public AggregateResult aggregate(KeyType key, int keyHash)
  {
    return delegate.aggregate(key, keyHash);
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

  private Entry<KeyType> buildSummaryRow()
  {
    final ReusableEntry<KeyType> reusableEntry = ReusableEntry.create(keySerde, aggregatorFactories.length);
    Object[] values = reusableEntry.getValues();
    for (int i = 0; i < aggregatorFactories.length; i++) {
      Aggregator aggregate = aggregatorFactories[i].factorize(columnSelectorFactory);
      values[i] = aggregate.get();
    }
    return reusableEntry;
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(boolean sorted)
  {
    final CloseableIterator<Entry<KeyType>> it = delegate.iterator(sorted);
    return new CloseableIterator<Grouper.Entry<KeyType>>()
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
      public Entry<KeyType> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        if (delegated) {
          return it.next();
        }
        done = true;
        return buildSummaryRow();
      }

      @Override
      public void close() throws IOException
      {
        it.close();
      }
    };
  }
}
