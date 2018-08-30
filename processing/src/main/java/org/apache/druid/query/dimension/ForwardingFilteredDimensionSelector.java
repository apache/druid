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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;

import javax.annotation.Nullable;
import java.util.BitSet;

final class ForwardingFilteredDimensionSelector extends AbstractDimensionSelector implements IdLookup
{
  private final DimensionSelector selector;
  private final IdLookup baseIdLookup;
  private final Int2IntOpenHashMap forwardMapping;
  private final int[] reverseMapping;
  private final ArrayBasedIndexedInts row = new ArrayBasedIndexedInts();

  /**
   * @param selector must return true from {@link DimensionSelector#nameLookupPossibleInAdvance()}
   * @param forwardMapping must have {@link Int2IntOpenHashMap#defaultReturnValue(int)} configured to -1.
   */
  ForwardingFilteredDimensionSelector(
      DimensionSelector selector,
      Int2IntOpenHashMap forwardMapping,
      int[] reverseMapping
  )
  {
    this.selector = Preconditions.checkNotNull(selector);
    if (!selector.nameLookupPossibleInAdvance()) {
      throw new IAE("selector.nameLookupPossibleInAdvance() should return true");
    }
    this.baseIdLookup = selector.idLookup();
    this.forwardMapping = Preconditions.checkNotNull(forwardMapping);
    if (forwardMapping.defaultReturnValue() != -1) {
      throw new IAE("forwardMapping.defaultReturnValue() should be -1");
    }
    this.reverseMapping = Preconditions.checkNotNull(reverseMapping);
  }

  @Override
  public IndexedInts getRow()
  {
    IndexedInts baseRow = selector.getRow();
    int baseRowSize = baseRow.size();
    row.ensureSize(baseRowSize);
    int resultSize = 0;
    for (int i = 0; i < baseRowSize; i++) {
      int forwardedValue = forwardMapping.get(baseRow.get(i));
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
      if (valueId >= 0 || value == null) {
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final IndexedInts baseRow = selector.getRow();
            final int baseRowSize = baseRow.size();
            boolean nullRow = true;
            for (int i = 0; i < baseRowSize; i++) {
              int forwardedValue = forwardMapping.get(baseRow.get(i));
              if (forwardedValue >= 0) {
                // Make the following check after the `forwardedValue >= 0` check, because if forwardedValue is -1 and
                // valueId is -1, we don't want to return true from matches().
                if (forwardedValue == valueId) {
                  return true;
                }
                nullRow = false;
              }
            }
            // null should match empty rows in multi-value columns
            return nullRow && value == null;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("selector", selector);
          }
        };
      } else {
        return BooleanValueMatcher.of(false);
      }
    } else {
      // Employ precomputed BitSet optimization
      return makeValueMatcher(Predicates.equalTo(value));
    }
  }

  @Override
  public ValueMatcher makeValueMatcher(Predicate<String> predicate)
  {
    final BitSet valueIds = DimensionSelectorUtils.makePredicateMatchingSet(this, predicate);
    final boolean matchNull = predicate.apply(null);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts baseRow = selector.getRow();
        final int baseRowSize = baseRow.size();
        boolean nullRow = true;
        for (int i = 0; i < baseRowSize; ++i) {
          int forwardedValue = forwardMapping.get(baseRow.get(i));
          if (forwardedValue >= 0) {
            if (valueIds.get(forwardedValue)) {
              return true;
            }
            nullRow = false;
          }
        }
        // null should match empty rows in multi-value columns
        return nullRow && matchNull;
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
    return forwardMapping.size();
  }

  @Override
  public String lookupName(int id)
  {
    return selector.lookupName(reverseMapping[id]);
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
    return forwardMapping.get(baseIdLookup.lookupId(name));
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
