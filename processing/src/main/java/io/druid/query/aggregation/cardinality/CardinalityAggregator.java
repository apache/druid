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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.druid.hll.HyperLogLogCollector;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;

import java.util.List;

public class CardinalityAggregator implements Aggregator
{
  private final String name;
  private final ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses;
  private final boolean byRow;

  public static final HashFunction hashFn = Hashing.murmur3_128();

  static void hashRow(
      ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses,
      HyperLogLogCollector collector
  )
  {
    final Hasher hasher = hashFn.newHasher();
    for (int k = 0; k < selectorPluses.length; ++k) {
      if (k != 0) {
        hasher.putByte((byte) 0);
      }

      ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy> selectorPlus = selectorPluses[k];
      selectorPlus.getColumnSelectorStrategy().hashRow(selectorPlus.getSelector(), hasher);
    }
    collector.add(hasher.hash().asBytes());
  }

  static void hashValues(
      ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses,
      HyperLogLogCollector collector
  )
  {
    for (final ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy> selectorPlus : selectorPluses) {
      selectorPlus.getColumnSelectorStrategy().hashValues(selectorPlus.getSelector(), collector);
    }
  }

  private HyperLogLogCollector collector;

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  CardinalityAggregator(
      String name,
      List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList,
      boolean byRow
  )
  {
    this(name, selectorPlusList.toArray(new ColumnSelectorPlus[] {}), byRow);
  }

  CardinalityAggregator(
      String name,
      ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses,
      boolean byRow
  )
  {
    this.name = name;
    this.selectorPluses = selectorPluses;
    this.collector = HyperLogLogCollector.makeLatestCollector();
    this.byRow = byRow;
  }

  @Override
  public void aggregate()
  {
    if (byRow) {
      hashRow(selectorPluses, collector);
    } else {
      hashValues(selectorPluses, collector);
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
    // Workaround for non-thread-safe use of HyperLogLogCollector.
    // OnheapIncrementalIndex has a penchant for calling "aggregate" and "get" simultaneously.
    return HyperLogLogCollector.makeCollectorSharingStorage(collector);
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
  public double getDouble()
  {
    throw new UnsupportedOperationException("CardinalityAggregator does not support getDouble()");
  }

  @Override
  public Aggregator clone()
  {
    return new CardinalityAggregator(name, selectorPluses, byRow);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
