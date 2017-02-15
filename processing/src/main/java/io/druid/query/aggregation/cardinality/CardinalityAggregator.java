/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.cardinality;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.druid.hll.HyperLogLogCollector;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;

import java.util.List;

public class CardinalityAggregator implements Aggregator
{
  private final String name;
  private final List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList;
  private final boolean byRow;

  public static final HashFunction hashFn = Hashing.murmur3_128();

  protected static void hashRow(
      List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList,
      HyperLogLogCollector collector
  )
  {
    final Hasher hasher = hashFn.newHasher();
    for (int k = 0; k < selectorPlusList.size(); ++k) {
      if (k != 0) {
        hasher.putByte((byte) 0);
      }

      ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy> selectorPlus = selectorPlusList.get(k);
      selectorPlus.getColumnSelectorStrategy().hashRow(selectorPlus.getSelector(), hasher);
    }
    collector.add(hasher.hash().asBytes());
  }

  protected static void hashValues(
      List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList,
      HyperLogLogCollector collector
  )
  {
    for (final ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy> selectorPlus : selectorPlusList) {
      selectorPlus.getColumnSelectorStrategy().hashValues(selectorPlus.getSelector(), collector);
    }
  }

  private HyperLogLogCollector collector;

  public CardinalityAggregator(
      String name,
      List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList,
      boolean byRow
  )
  {
    this.name = name;
    this.selectorPlusList = selectorPlusList;
    this.collector = HyperLogLogCollector.makeLatestCollector();
    this.byRow = byRow;
  }

  @Override
  public void aggregate()
  {
    if (byRow) {
      hashRow(selectorPlusList, collector);
    } else {
      hashValues(selectorPlusList, collector);
    }
  }

  @Override
  public void reset()
  {
    collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public Object get()
  {
    return collector;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("CardinalityAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("CardinalityAggregator does not support getLong()");
  }

  @Override
  public Aggregator clone()
  {
    return new CardinalityAggregator(name, selectorPlusList, byRow);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
