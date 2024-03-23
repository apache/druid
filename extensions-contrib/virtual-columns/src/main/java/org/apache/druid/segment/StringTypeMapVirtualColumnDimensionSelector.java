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

package org.apache.druid.segment;

import com.google.common.base.Preconditions;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.data.ZeroIndexedInts;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * {@link DimensionSelector} for String type {@link MapVirtualColumn}. The performance has not considered yet and so
 * it may need to be improved later.
 */
final class StringTypeMapVirtualColumnDimensionSelector extends MapVirtualColumnDimensionSelector
{
  private final String subColumnName;
  private final SingleIndexedInt indexedInt = new SingleIndexedInt();

  StringTypeMapVirtualColumnDimensionSelector(
      DimensionSelector keySelector,
      DimensionSelector valueSelector,
      String subColumnName
  )
  {
    super(keySelector, valueSelector);
    this.subColumnName = Preconditions.checkNotNull(subColumnName, "subColumnName");
  }

  @Override
  public IndexedInts getRow()
  {
    final int valueIndex = findValueIndicesIndexForSubColumn();
    if (valueIndex < 0) {
      return ZeroIndexedInts.instance();
    } else {
      indexedInt.setValue(valueIndex);
      return indexedInt;
    }
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final Object rowValue = getObject();
        return (includeUnknown && rowValue == null) || Objects.equals(value, rowValue);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("keySelector", getKeySelector());
        inspector.visit("valueSelector", getValueSelector());
        inspector.visit("subColumnName", subColumnName);
      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final String rowValue = (String) getObject();
        return predicate.apply(rowValue).matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("keySelector", getKeySelector());
        inspector.visit("valueSelector", getValueSelector());
        inspector.visit("subColumnName", subColumnName);
      }
    };
  }

  @Override
  public int getValueCardinality()
  {
    // To get the value cardinarlity, we need to first check all keys and values to find valid pairs, and then find the
    // number of distinct values among them.
    return CARDINALITY_UNKNOWN;
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    final int valueIndex = findValueIndicesIndexForSubColumn();

    if (valueIndex == id) {
      return getValueSelector().lookupName(id);
    } else {
      return null;
    }
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return false;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    final DimensionSelector valueSelector = getValueSelector();
    final IdLookup valueLookup = valueSelector.idLookup();

    if (valueLookup != null) {
      final int valueIndex = findValueIndicesIndexForSubColumn();
      return name -> {
        final int candidate = valueLookup.lookupId(name);
        if (candidate == valueIndex) {
          return candidate;
        }
        return -1;
      };
    } else {
      return null;
    }
  }

  @Nullable
  @Override
  public Object getObject()
  {
    final int valueIndex = findValueIndicesIndexForSubColumn();

    if (valueIndex < 0) {
      return null;
    } else {
      final DimensionSelector valueSelector = getValueSelector();
      final IndexedInts valueIndices = valueSelector.getRow();
      return valueSelector.lookupName(valueIndices.get(valueIndex));
    }
  }

  /**
   * Find the index of valueIndices which is {@link IndexedInts} returned from {@link #getValueSelector()#getRow()}
   * corresponding to the {@link #subColumnName}.
   *
   * @return index for valueIndices if found. -1 otherwise.
   */
  private int findValueIndicesIndexForSubColumn()
  {
    final DimensionSelector keySelector = getKeySelector();
    final DimensionSelector valueSelector = getValueSelector();

    final IndexedInts keyIndices = keySelector.getRow();
    final IndexedInts valueIndices = valueSelector.getRow();

    final int limit = Math.min(keyIndices.size(), valueIndices.size());

    return IntStream
        .range(0, limit)
        .filter(i -> subColumnName.equals(keySelector.lookupName(keyIndices.get(i)))) // subColumnName is never null
        .findAny()
        .orElse(-1);
  }

  @Override
  public Class classOfObject()
  {
    return String.class;
  }
}
