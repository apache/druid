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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.IdMapping;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.BitSet;

final class ForwardingFilteredDimensionSelector extends AbstractDimensionSelector implements IdLookup
{
  private final DimensionSelector selector;
  private final IdLookup baseIdLookup;
  private final IdMapping idMapping;
  private final ArrayBasedIndexedInts row = new ArrayBasedIndexedInts();

  /**
   * @param selector must return true from {@link DimensionSelector#nameLookupPossibleInAdvance()}
   * @param idMapping must have be initialized and populated with the dictionary id mapping.
   */
  public ForwardingFilteredDimensionSelector(
      DimensionSelector selector,
      IdMapping idMapping
  )
  {
    this.selector = Preconditions.checkNotNull(selector);
    if (!selector.nameLookupPossibleInAdvance()) {
      throw new IAE("selector.nameLookupPossibleInAdvance() should return true");
    }
    this.baseIdLookup = selector.idLookup();
    this.idMapping = Preconditions.checkNotNull(idMapping);
  }

  @Override
  public IndexedInts getRow()
  {
    IndexedInts baseRow = selector.getRow();
    int baseRowSize = baseRow.size();
    row.ensureSize(baseRowSize);
    int resultSize = 0;
    for (int i = 0; i < baseRowSize; i++) {
      int forwardedValue = idMapping.getForwardedId(baseRow.get(i));
      if (forwardedValue >= 0) {
        row.setValue(resultSize, forwardedValue);
        resultSize++;
      }
    }
    row.setSize(resultSize);
    return row;
  }

  @Override
  public ValueMatcher makeValueMatcher(final String value)
  {
    IdLookup idLookup = idLookup();
    if (idLookup != null) {
      final int valueId = idLookup.lookupId(value);
      final int nullId = baseIdLookup.lookupId(null);
      return new ValueMatcher()
      {
        @Override
        public boolean matches(boolean includeUnknown)
        {
          final IndexedInts baseRow = selector.getRow();
          final int baseRowSize = baseRow.size();
          boolean nullRow = true;
          for (int i = 0; i < baseRowSize; i++) {
            final int baseId = baseRow.get(i);
            if (includeUnknown && nullId == baseId) {
              return true;
            }
            final int forwardedId = idMapping.getForwardedId(baseId);
            if (forwardedId >= 0) {
              // Make the following check after the `forwardedId >= 0` check, because if forwardedId is -1 and
              // valueId is -1, we don't want to return true from matches().
              if (forwardedId == valueId) {
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
          inspector.visit("selector", selector);
        }
      };
    } else {
      // Employ precomputed BitSet optimization
      return makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(value));
    }
  }

  @Override
  public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
    final Supplier<BitSet> valueIds = Suppliers.memoize(
        () -> DimensionSelectorUtils.makePredicateMatchingSet(this, predicate, false)
    );
    final Supplier<BitSet> valueIdsWithUnknown = Suppliers.memoize(
        () -> DimensionSelectorUtils.makePredicateMatchingSet(this, predicate, true)
    );
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final IndexedInts baseRow = selector.getRow();
        final int baseRowSize = baseRow.size();
        boolean nullRow = true;
        for (int i = 0; i < baseRowSize; ++i) {
          final int baseId = baseRow.get(i);
          int forwardedValue = idMapping.getForwardedId(baseId);
          if (forwardedValue >= 0) {
            if (includeUnknown) {
              if (valueIdsWithUnknown.get().get(forwardedValue)) {
                return true;
              }
            } else if (valueIds.get().get(forwardedValue)) {
              return true;
            }
            nullRow = false;
          }
        }
        // null should match empty rows in multi-value columns
        return nullRow && predicate.apply(null).matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  @Override
  public int getValueCardinality()
  {
    return idMapping.getValueCardinality();
  }

  @Override
  public String lookupName(int id)
  {
    return selector.lookupName(idMapping.getReverseId(id));
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return true;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return baseIdLookup != null ? this : null;
  }

  @Override
  public int lookupId(String name)
  {
    return idMapping.getForwardedId(baseIdLookup.lookupId(name));
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
  }
}
