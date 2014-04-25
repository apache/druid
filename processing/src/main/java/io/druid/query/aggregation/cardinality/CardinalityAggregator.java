/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.cardinality;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Arrays;
import java.util.List;

public class CardinalityAggregator implements Aggregator
{
  private static final String NULL_STRING = "\u0000";

  private final String name;
  private final List<DimensionSelector> selectorList;
  private final boolean byRow;

  private static final HashFunction hashFn = Hashing.murmur3_128();
  public static final char SEPARATOR = '\u0001';

  protected static void hashRow(List<DimensionSelector> selectorList, HyperLogLogCollector collector)
  {
    final Hasher hasher = hashFn.newHasher();
    for (int k = 0; k < selectorList.size(); ++k) {
      if (k != 0) {
        hasher.putByte((byte) 0);
      }
      final DimensionSelector selector = selectorList.get(k);
      final IndexedInts row = selector.getRow();
      final int size = row.size();
      // nothing to add to hasher if size == 0, only handle size == 1 and size != 0 cases.
      if (size == 1) {
        final String value = selector.lookupName(row.get(0));
        hasher.putString(value != null ? value : NULL_STRING);
      } else if (size != 0) {
        final String[] values = new String[size];
        for (int i = 0; i < size; ++i) {
          final String value = selector.lookupName(row.get(i));
          values[i] = value != null ? value : NULL_STRING;
        }
        // Values need to be sorted to ensure consistent multi-value ordering across different segments
        Arrays.sort(values);
        for (int i = 0; i < size; ++i) {
          if (i != 0) {
            hasher.putChar(SEPARATOR);
          }
          hasher.putString(values[i]);
        }
      }
    }
    collector.add(hasher.hash().asBytes());
  }

  protected static void hashValues(final List<DimensionSelector> selectors, HyperLogLogCollector collector)
  {
    for (final DimensionSelector selector : selectors) {
      for (final Integer index : selector.getRow()) {
        final String value = selector.lookupName(index);
        collector.add(hashFn.hashString(value == null ? NULL_STRING : value).asBytes());
      }
    }
  }

  private HyperLogLogCollector collector;

  public CardinalityAggregator(
      String name,
      List<DimensionSelector> selectorList,
      boolean byRow
  )
  {
    this.name = name;
    this.selectorList = selectorList;
    this.collector = HyperLogLogCollector.makeLatestCollector();
    this.byRow = byRow;
  }

  @Override
  public void aggregate()
  {
    if (byRow) {
      hashRow(selectorList, collector);
    } else {
      hashValues(selectorList, collector);
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
  public String getName()
  {
    return name;
  }

  @Override
  public Aggregator clone()
  {
    return new CardinalityAggregator(name, selectorList, byRow);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
