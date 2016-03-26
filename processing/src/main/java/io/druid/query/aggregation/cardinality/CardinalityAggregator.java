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
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedLongs;

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
      int rowSize = 0;
      IndexedInts intVals = null;
      IndexedLongs longVals = null;
      IndexedFloats floatVals = null;
      Comparable objVal = null;
      ValueType type = selector.getDimCapabilities().getType();

      switch (type) {
        case STRING:
          intVals = selector.getRow();
          rowSize = intVals.size();
          if (rowSize == 1) {
            final String value = selector.lookupName(intVals.get(0));
            hasher.putUnencodedChars(value != null ? value : NULL_STRING);
          } else if (rowSize != 0) {
            final String[] values = new String[rowSize];
            for (int i = 0; i < rowSize; ++i) {
              final String value = selector.lookupName(intVals.get(i));
              values[i] = value != null ? value : NULL_STRING;
            }
            // Values need to be sorted to ensure consistent multi-value ordering across different segments
            Arrays.sort(values);
            for (int i = 0; i < rowSize; ++i) {
              if (i != 0) {
                hasher.putChar(SEPARATOR);
              }
              hasher.putUnencodedChars(values[i]);
            }
          }
          break;
        case LONG:
          longVals = selector.getLongRow();
          rowSize = longVals.size();
          if (rowSize > 0) {
            hasher.putLong(longVals.get(0));
          }
          break;
        case FLOAT:
          floatVals = selector.getFloatRow();
          rowSize = floatVals.size();
          if (rowSize > 0) {
            hasher.putFloat(floatVals.get(0));
          }
          break;
        case COMPLEX:
          objVal = selector.getComparableRow();
          hasher.putUnencodedChars(objVal != null ? objVal.toString() : NULL_STRING);
          break;
        default:
          throw new IAE("Invalid type: " + selector.getDimCapabilities().getType());
      }
    }
    collector.add(hasher.hash().asBytes());
  }

  protected static void hashValues(final List<DimensionSelector> selectors, HyperLogLogCollector collector)
  {
    for (final DimensionSelector selector : selectors) {
      switch(selector.getDimCapabilities().getType()) {
        case STRING:
          for (final Integer index : selector.getRow()) {
            final String value = selector.lookupName(index);
            collector.add(hashFn.hashUnencodedChars(value == null ? NULL_STRING : value).asBytes());
          }
          break;
        case LONG:
          long longVal = selector.getLongRow().get(0);
          collector.add(hashFn.hashBytes(Longs.toByteArray(longVal)).asBytes());
          break;
        case FLOAT:
          float floatVal = selector.getFloatRow().get(0);
          collector.add(hashFn.hashBytes(Ints.toByteArray(Float.floatToIntBits(floatVal))).asBytes());
          break;
        case COMPLEX:
          //TODO: pass in handler and call get Bytes
          break;
        default:
          break;
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
  public long getLong()
  {
    throw new UnsupportedOperationException("CardinalityAggregator does not support getLong()");
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
