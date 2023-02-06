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
import com.google.common.base.Predicate;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Iterator;

/**
 * Basic structure for indexing dictionary encoded columns
 */
public abstract class DictionaryEncodedColumnIndexer<KeyType, ActualType extends Comparable<ActualType>>
    implements DimensionIndexer<Integer, KeyType, ActualType>
{
  protected final DimensionDictionary<ActualType> dimLookup;
  protected volatile boolean isSparse = false;

  @Nullable
  protected SortedDimensionDictionary<ActualType> sortedLookup;

  /**
   * Creates a new DictionaryEncodedColumnIndexer.
   *
   * @param dimLookup Dimension Dictionary to lookup dimension values.
   */
  public DictionaryEncodedColumnIndexer(@NotNull DimensionDictionary<ActualType> dimLookup)
  {
    this.dimLookup = Preconditions.checkNotNull(dimLookup);
  }

  @Override
  public void setSparseIndexed()
  {
    isSparse = true;
  }

  public int getSortedEncodedValueFromUnsorted(Integer unsortedIntermediateValue)
  {
    return sortedLookup().getSortedIdFromUnsortedId(unsortedIntermediateValue);
  }

  @Override
  public Integer getUnsortedEncodedValueFromSorted(Integer sortedIntermediateValue)
  {
    return sortedLookup().getUnsortedIdFromSortedId(sortedIntermediateValue);
  }

  @Override
  public CloseableIndexed<ActualType> getSortedIndexedValues()
  {
    return new CloseableIndexed<ActualType>()
    {
      @Override
      public int size()
      {
        return getCardinality();
      }

      @Override
      public ActualType get(int index)
      {
        return getActualValue(index, true);
      }

      @Override
      public int indexOf(ActualType value)
      {
        int id = getEncodedValue(value, false);
        return id < 0 ? DimensionDictionary.ABSENT_VALUE_ID : getSortedEncodedValueFromUnsorted(id);
      }

      @Override
      public Iterator<ActualType> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }

      @Override
      public void close()
      {
        // nothing to close
      }
    };
  }

  @Override
  public ActualType getMinValue()
  {
    return dimLookup.getMinValue();
  }

  @Override
  public ActualType getMaxValue()
  {
    return dimLookup.getMaxValue();
  }

  @Override
  public int getCardinality()
  {
    return dimLookup.size();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    return makeDimensionSelector(desc.getHandler().getDimensionSpec(), currEntry, desc);
  }

  @Override
  public ColumnValueSelector convertUnsortedValuesToSorted(ColumnValueSelector selectorWithUnsortedValues)
  {
    DimensionSelector dimSelectorWithUnsortedValues = (DimensionSelector) selectorWithUnsortedValues;
    class SortedDimensionSelector implements DimensionSelector, IndexedInts
    {
      @Override
      public int size()
      {
        return dimSelectorWithUnsortedValues.getRow().size();
      }

      @Override
      public int get(int index)
      {
        return sortedLookup().getSortedIdFromUnsortedId(dimSelectorWithUnsortedValues.getRow().get(index));
      }

      @Override
      public IndexedInts getRow()
      {
        return this;
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getValueCardinality()
      {
        return dimSelectorWithUnsortedValues.getValueCardinality();
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("dimSelectorWithUnsortedValues", dimSelectorWithUnsortedValues);
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return dimSelectorWithUnsortedValues.getObject();
      }

      @Override
      public Class classOfObject()
      {
        return dimSelectorWithUnsortedValues.classOfObject();
      }
    }
    return new SortedDimensionSelector();
  }

  /**
   * returns true if all values are encoded in {@link #dimLookup}
   */
  protected boolean dictionaryEncodesAllValues()
  {
    // name lookup is possible in advance if we explicitly process a value for every row, or if we've encountered an
    // actual null value and it is present in our dictionary. otherwise the dictionary will be missing ids for implicit
    // null values
    return !isSparse || dimLookup.getIdForNull() != DimensionDictionary.ABSENT_VALUE_ID;
  }

  protected SortedDimensionDictionary<ActualType> sortedLookup()
  {
    return sortedLookup == null ? sortedLookup = dimLookup.sort() : sortedLookup;
  }

  @Nullable
  protected ActualType getActualValue(int intermediateValue, boolean idSorted)
  {
    if (idSorted) {
      return sortedLookup().getValueFromSortedId(intermediateValue);
    } else {
      return dimLookup.getValue(intermediateValue);

    }
  }

  protected int getEncodedValue(@Nullable ActualType fullValue, boolean idSorted)
  {
    int unsortedId = dimLookup.getId(fullValue);

    if (idSorted) {
      return sortedLookup().getSortedIdFromUnsortedId(unsortedId);
    } else {
      return unsortedId;
    }
  }
}
