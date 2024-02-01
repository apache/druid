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

package org.apache.druid.query.dimension;

import com.google.common.base.Predicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Objects;

final class PredicateFilteredDimensionSelector extends AbstractDimensionSelector
{
  private final DimensionSelector selector;
  private final Predicate<String> predicate;
  private final ArrayBasedIndexedInts row = new ArrayBasedIndexedInts();

  PredicateFilteredDimensionSelector(DimensionSelector selector, Predicate<String> predicate)
  {
    this.selector = selector;
    this.predicate = predicate;
  }

  @Override
  public IndexedInts getRow()
  {
    IndexedInts baseRow = selector.getRow();
    int baseRowSize = baseRow.size();
    row.ensureSize(baseRowSize);
    int resultSize = 0;
    for (int i = 0; i < baseRowSize; i++) {
      int id = baseRow.get(i);
      if (predicate.apply(selector.lookupName(id))) {
        row.setValue(resultSize, id);
        resultSize++;
      }
    }
    row.setSize(resultSize);
    return row;
  }

  @Override
  public ValueMatcher makeValueMatcher(final String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final IndexedInts baseRow = selector.getRow();
        final int baseRowSize = baseRow.size();
        boolean nullRow = true;
        for (int i = 0; i < baseRowSize; i++) {
          String rowValue = lookupName(baseRow.get(i));
          if (includeUnknown && rowValue == null) {
            return true;
          }
          if (predicate.apply(rowValue)) {
            if (Objects.equals(rowValue, value)) {
              return true;
            }
            nullRow = false;
          }
        }
        // null should match empty rows in multi-value columns
        return nullRow && (includeUnknown || value == null);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // PredicateFilteredDimensionSelector.this inspects selector and predicate as well.
        inspector.visit("selector", PredicateFilteredDimensionSelector.this);
      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(final DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<String> matcherPredicate = predicateFactory.makeStringPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final IndexedInts baseRow = selector.getRow();
        final int baseRowSize = baseRow.size();
        boolean nullRow = true;
        for (int i = 0; i < baseRowSize; ++i) {
          String rowValue = lookupName(baseRow.get(i));
          if (predicate.apply(rowValue)) {
            if (matcherPredicate.apply(rowValue).matches(includeUnknown)) {
              return true;
            }
            nullRow = false;
          }
        }
        // null should match empty rows in multi-value columns
        return nullRow && matcherPredicate.apply(null).matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // PredicateFilteredDimensionSelector.this inspects selector and predicate as well.
        inspector.visit("selector", PredicateFilteredDimensionSelector.this);
        inspector.visit("matcherPredicate", matcherPredicate);
      }
    };
  }

  @Override
  public int getValueCardinality()
  {
    return selector.getValueCardinality();
  }

  @Override
  public String lookupName(int id)
  {
    return selector.lookupName(id);
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return selector.nameLookupPossibleInAdvance();
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return selector.idLookup();
  }

  @Override
  public Class classOfObject()
  {
    return Object.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("predicate", predicate);
  }
}
