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

package org.apache.druid.query.aggregation.cardinality;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;

import java.util.List;

public class CardinalityAggregator implements Aggregator
{
  public static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

  static void hashRow(
      ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses,
      HyperLogLogCollector collector
  )
  {
    final Hasher hasher = HASH_FUNCTION.newHasher();
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

  private final ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses;
  private final boolean byRow;
  private HyperLogLogCollector collector;

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  CardinalityAggregator(
      List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList,
      boolean byRow
  )
  {
    this(selectorPlusList.toArray(new ColumnSelectorPlus[0]), byRow);
  }

  CardinalityAggregator(ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses, boolean byRow)
  {
    this.selectorPluses = selectorPluses;
    this.collector = HyperLogLogCollector.makeLatestCollector();
    this.byRow = byRow;
  }

  @Override
  public synchronized void aggregate()
  {
    if (byRow) {
      hashRow(selectorPluses, collector);
    } else {
      hashValues(selectorPluses, collector);
    }
  }

  @Override
  public synchronized Object get()
  {
    // Must make a new collector duplicating the underlying buffer to ensure the object from "get" is usable
    // in a thread-safe manner.
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
  public void close()
  {
    // no resources to cleanup
  }
}
