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

package io.druid.query.aggregation.cardinality.types;

import com.google.common.hash.Hasher;
import io.druid.hll.HyperLogLogCollector;
import io.druid.query.aggregation.cardinality.CardinalityAggregator;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Arrays;

public class StringCardinalityAggregatorColumnSelectorStrategy implements CardinalityAggregatorColumnSelectorStrategy<DimensionSelector>
{
  public static final String CARDINALITY_AGG_NULL_STRING = "\u0000";
  public static final char CARDINALITY_AGG_SEPARATOR = '\u0001';

  @Override
  public void hashRow(DimensionSelector dimSelector, Hasher hasher)
  {
    final IndexedInts row = dimSelector.getRow();
    final int size = row.size();
    // nothing to add to hasher if size == 0, only handle size == 1 and size != 0 cases.
    if (size == 1) {
      final String value = dimSelector.lookupName(row.get(0));
      hasher.putUnencodedChars(nullToSpecial(value));
    } else if (size != 0) {
      final String[] values = new String[size];
      for (int i = 0; i < size; ++i) {
        final String value = dimSelector.lookupName(row.get(i));
        values[i] = nullToSpecial(value);
      }
      // Values need to be sorted to ensure consistent multi-value ordering across different segments
      Arrays.sort(values);
      for (int i = 0; i < size; ++i) {
        if (i != 0) {
          hasher.putChar(CARDINALITY_AGG_SEPARATOR);
        }
        hasher.putUnencodedChars(values[i]);
      }
    }
  }

  @Override
  public void hashValues(DimensionSelector dimSelector, HyperLogLogCollector collector)
  {
    IndexedInts row = dimSelector.getRow();
    for (int i = 0; i < row.size(); i++) {
      int index = row.get(i);
      final String value = dimSelector.lookupName(index);
      collector.add(CardinalityAggregator.hashFn.hashUnencodedChars(nullToSpecial(value)).asBytes());
    }
  }

  private String nullToSpecial(String value)
  {
    return value == null ? CARDINALITY_AGG_NULL_STRING : value;
  }
}
