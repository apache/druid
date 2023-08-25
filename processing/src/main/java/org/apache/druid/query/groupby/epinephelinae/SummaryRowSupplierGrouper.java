/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.druid.query.groupby.epinephelinae;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;

public class SummaryRowSupplierGrouper<KeyType> implements Grouper<KeyType>
{
  private Grouper<KeyType> delegate;
  private KeySerde<KeyType> keySerde;
  private AggregatorFactory[] aggregatorFactories;
  private ColumnSelectorFactory columnSelectorFactory;

  public SummaryRowSupplierGrouper(Grouper<KeyType> grouper, KeySerdeFactory<KeyType> keySerdeFactory,
      ColumnSelectorFactory columnSelectorFactory, AggregatorFactory[] aggregatorFactories)
  {
    delegate = grouper;
    this.keySerde = keySerdeFactory.factorize();
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

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(boolean sorted)
  {
    CloseableIterator<Entry<KeyType>> it = delegate.iterator(sorted);
    if (it.hasNext()) {
      return it;
    }
    Entry<KeyType> summaryRow = buildSummaryRow();
    return new CloseableIterator<Grouper.Entry<KeyType>>()
    {
      boolean done;

      @Override
      public boolean hasNext()
      {
        return !done;
      }

      @Override
      public Entry<KeyType> next()
      {
        if (done) {
          throw new NoSuchElementException();
        }
        done = true;
        return summaryRow;
      }

      @Override
      public void close() throws IOException
      {
        it.close();
      }
    };
  }

  private Entry<KeyType> buildSummaryRow()
  {
    final ReusableEntry<KeyType> reusableEntry = ReusableEntry.create(keySerde, aggregatorFactories.length);
    Object[] values = reusableEntry.getValues();// new
                                                // Object[aggregatorFactories.length];
    for (int i = 0; i < aggregatorFactories.length; i++) {
      Aggregator aggregate = aggregatorFactories[i].factorize(columnSelectorFactory);
      values[i] = aggregate.get();
    }
    // reusableEntry.setValues(values);
    return reusableEntry;

    //
    //
    // int curr = 0;
    // final int size = getSize();
    //
    // @Override
    // public boolean hasNext()
    // {
    // return curr < size;
    // }
    //
    // @Override
    // public Entry<KeyType> next()
    // {
    // if (curr >= size) {
    // throw new NoSuchElementException();
    // }
    // final int offset = offsetList.get(curr);
    // final Entry<KeyType> entry = populateBucketEntryForOffset(reusableEntry,
    // offset);
    // curr++;
    //
    // return entry;
    // }

    // AggregatorAdapters.factorizeBuffered(columnSelectorFactory,
    // Arrays.asList(aggregatorFactories));

  }

}
