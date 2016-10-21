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

package io.druid.segment;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringDimensionIndexer implements DimensionIndexer<Integer, int[], String>
{
  private static final Logger log = new Logger(StringDimensionIndexer.class);

  private static class DimensionDictionary
  {
    private String minValue = null;
    private String maxValue = null;

    private final Map<String, Integer> valueToId = Maps.newHashMap();

    private final List<String> idToValue = Lists.newArrayList();
    private final Object lock;

    public DimensionDictionary()
    {
      this.lock = new Object();
    }

    public int getId(String value)
    {
      synchronized (lock) {
        final Integer id = valueToId.get(Strings.nullToEmpty(value));
        return id == null ? -1 : id;
      }
    }

    public String getValue(int id)
    {
      synchronized (lock) {
        return Strings.emptyToNull(idToValue.get(id));
      }
    }

    public boolean contains(String value)
    {
      synchronized (lock) {
        return valueToId.containsKey(value);
      }
    }

    public int size()
    {
      synchronized (lock) {
        return valueToId.size();
      }
    }

    public int add(String originalValue)
    {
      String value = Strings.nullToEmpty(originalValue);
      synchronized (lock) {
        Integer prev = valueToId.get(value);
        if (prev != null) {
          return prev;
        }
        final int index = size();
        valueToId.put(value, index);
        idToValue.add(value);
        minValue = minValue == null || minValue.compareTo(value) > 0 ? value : minValue;
        maxValue = maxValue == null || maxValue.compareTo(value) < 0 ? value : maxValue;
        return index;
      }
    }

    public String getMinValue()
    {
      return minValue;
    }

    public String getMaxValue()
    {
      return maxValue;
    }

    public SortedDimensionDictionary sort()
    {
      synchronized (lock) {
        return new SortedDimensionDictionary(idToValue, size());
      }
    }
  }

  private static class SortedDimensionDictionary
  {
    private final List<String> sortedVals;
    private final int[] idToIndex;
    private final int[] indexToId;

    public SortedDimensionDictionary(List<String> idToValue, int length)
    {
      Map<String, Integer> sortedMap = Maps.newTreeMap();
      for (int id = 0; id < length; id++) {
        sortedMap.put(idToValue.get(id), id);
      }
      this.sortedVals = Lists.newArrayList(sortedMap.keySet());
      this.idToIndex = new int[length];
      this.indexToId = new int[length];
      int index = 0;
      for (Integer id : sortedMap.values()) {
        idToIndex[id] = index;
        indexToId[index] = id;
        index++;
      }
    }

    public int size()
    {
      return sortedVals.size();
    }

    public int getUnsortedIdFromSortedId(int index)
    {
      return indexToId[index];
    }

    public int getSortedIdFromUnsortedId(int id)
    {
      return idToIndex[id];
    }

    public String getValueFromSortedId(int index)
    {
      return Strings.emptyToNull(sortedVals.get(index));
    }
  }

  private DimensionDictionary dimLookup;
  private SortedDimensionDictionary sortedLookup;

  public StringDimensionIndexer()
  {
    this.dimLookup = new DimensionDictionary();
  }

  @Override
  public int[] processRowValsToUnsortedEncodedArray(Object dimValues)
  {
    final int[] encodedDimensionValues;
    final int oldDictSize = dimLookup.size();

    if (dimValues == null) {
      dimLookup.add(null);
      encodedDimensionValues = null;
    } else if (dimValues instanceof List) {
      List<Object> dimValuesList = (List) dimValues;

      // Sort multival row by their unencoded values first.
      final String[] dimensionValues = new String[dimValuesList.size()];
      for (int i = 0; i < dimValuesList.size(); i++) {
        dimensionValues[i] = StringDimensionHandler.STRING_TRANSFORMER.apply(dimValuesList.get(i));
      }
      Arrays.sort(dimensionValues, StringDimensionHandler.UNENCODED_COMPARATOR);

      encodedDimensionValues = new int[dimensionValues.length];
      for (int i = 0; i < dimensionValues.length; i++) {
        encodedDimensionValues[i] = dimLookup.add(dimensionValues[i]);
      }
    } else {
      String transformedVal = StringDimensionHandler.STRING_TRANSFORMER.apply(dimValues);
      encodedDimensionValues = new int[]{dimLookup.add(transformedVal)};
    }

    // If dictionary size has changed, the sorted lookup is no longer valid.
    if (oldDictSize != dimLookup.size()) {
      sortedLookup = null;
    }

    return encodedDimensionValues;
  }

  @Override
  public Integer getSortedEncodedValueFromUnsorted(Integer unsortedIntermediateValue)
  {
    updateSortedLookup();
    return sortedLookup.getSortedIdFromUnsortedId(unsortedIntermediateValue);
  }

  @Override
  public Integer getUnsortedEncodedValueFromSorted(Integer sortedIntermediateValue)
  {
    updateSortedLookup();
    return sortedLookup.getUnsortedIdFromSortedId(sortedIntermediateValue);
  }

  @Override
  public Indexed<String> getSortedIndexedValues()
  {
    updateSortedLookup();
    return new Indexed<String>()
    {
      @Override
      public Class<? extends String> getClazz()
      {
        return String.class;
      }

      @Override
      public int size()
      {
        return getCardinality();
      }

      @Override
      public String get(int index)
      {
        return getActualValue(index, true);
      }

      @Override
      public int indexOf(String value)
      {
        int id = getEncodedValue(value, false);
        return id < 0 ? -1 : getSortedEncodedValueFromUnsorted(id);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }
    };
  }

  @Override
  public String getMinValue()
  {
    return dimLookup.getMinValue();
  }

  @Override
  public String getMaxValue()
  {
    return dimLookup.getMaxValue();
  }

  @Override
  public int getCardinality()
  {
    return dimLookup.size();
  }

  @Override
  public int compareUnsortedEncodedArrays(int[] lhs, int[] rhs)
  {
    int lhsLen = lhs.length;
    int rhsLen = rhs.length;

    int retVal = Ints.compare(lhsLen, rhsLen);
    int valsIndex = 0;
    while (retVal == 0 && valsIndex < lhsLen) {
      int lhsVal = lhs[valsIndex];
      int rhsVal = rhs[valsIndex];
      if (lhsVal != rhsVal) {
        final String lhsValActual = getActualValue(lhsVal, false);
        final String rhsValActual = getActualValue(rhsVal, false);
        if (lhsValActual != null && rhsValActual != null) {
          retVal = lhsValActual.compareTo(rhsValActual);
        } else if (lhsValActual == null ^ rhsValActual == null) {
          retVal = lhsValActual == null ? -1 : 1;
        }
      }
      ++valsIndex;
    }
    return retVal;
  }

  @Override
  public boolean checkUnsortedEncodedArraysEqual(int[] lhs, int[] rhs)
  {
    return Arrays.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedArrayHashCode(int[] key)
  {
    return Arrays.hashCode(key);
  }

  @Override
  public Object makeColumnValueSelector(
      final DimensionSpec spec,
      final IncrementalIndexStorageAdapter.EntryHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final ExtractionFn extractionFn = spec.getExtractionFn();

    final int dimIndex = desc.getIndex();
    final int maxId = getCardinality();

    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        final Object[] dims = currEntry.getKey().getDims();

        int[] indices;
        if (dimIndex < dims.length) {
          indices = (int[]) dims[dimIndex];
        } else {
          indices = null;
        }

        int nullId = getEncodedValue(null, false);
        IntList valsTmp = null;
        if ((indices == null || indices.length == 0) && nullId > -1) {
          if (nullId < maxId) {
            valsTmp = IntLists.singleton(nullId);
          }
        } else if (indices != null && indices.length > 0) {
          valsTmp = new IntArrayList(indices.length);
          for (int i = 0; i < indices.length; i++) {
            int id = indices[i];
            if (id < maxId) {
              valsTmp.add(id);
            }
          }
        }

        final IntList vals = valsTmp == null ? IntLists.EMPTY_LIST : valsTmp;
        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return vals.size();
          }

          @Override
          public int get(int index)
          {
            return vals.get(index);
          }

          @Override
          public IntIterator iterator()
          {
            return vals.iterator();
          }

          @Override
          public void fill(int index, int[] toFill)
          {
            throw new UnsupportedOperationException("fill not supported");
          }

          @Override
          public void close() throws IOException
          {

          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return maxId;
      }

      @Override
      public String lookupName(int id)
      {
        final String strValue = getActualValue(id, false);
        return extractionFn == null ? strValue : extractionFn.apply(strValue);
      }

      @Override
      public int lookupId(String name)
      {
        if (extractionFn != null) {
          throw new UnsupportedOperationException(
              "cannot perform lookup when applying an extraction function"
          );
        }
        return getEncodedValue(name, false);
      }
    };
  }

  @Override
  public Object convertUnsortedEncodedArrayToActualArrayOrList(int[] key, boolean asList)
  {
    if (key == null || key.length == 0) {
      return null;
    }
    if (key.length == 1) {
      String val = getActualValue(key[0], false);
      val = Strings.nullToEmpty(val);
      return val;
    } else {
      if (asList) {
        List<Comparable> rowVals = new ArrayList<>(key.length);
        for (int i = 0; i < key.length; i++) {
          String val = getActualValue(key[i], false);
          rowVals.add(Strings.nullToEmpty(val));
        }
        return rowVals;
      } else {
        String[] rowArray = new String[key.length];
        for (int i = 0; i < key.length; i++) {
          String val = getActualValue(key[i], false);
          rowArray[i] = Strings.nullToEmpty(val);
        }
        return rowArray;
      }
    }
  }

  @Override
  public int[] convertUnsortedEncodedArrayToSortedEncodedArray(int[] key)
  {
    int[] sortedDimVals = new int[key.length];
    for (int i = 0; i < key.length; ++i) {
      // The encoded values in the TimeAndDims key are not sorted based on their final unencoded values, so need this lookup.
      sortedDimVals[i] = getSortedEncodedValueFromUnsorted(key[i]);
    }
    return sortedDimVals;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedArray(
      int[] key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    for (int dimValIdx : key) {
      if (bitmapIndexes[dimValIdx] == null) {
        bitmapIndexes[dimValIdx] = factory.makeEmptyMutableBitmap();
      }
      bitmapIndexes[dimValIdx].add(rowNum);
    }
  }

  @Override
  public ValueMatcher makeIndexingValueMatcher(
      final Comparable matchValue,
      final IncrementalIndexStorageAdapter.EntryHolder holder,
      final int dimIndex
  )
  {
    final String value = StringDimensionHandler.STRING_TRANSFORMER.apply(matchValue);
    final int encodedVal = getEncodedValue(value, false);
    final boolean matchOnNull = Strings.isNullOrEmpty(value);
    if (encodedVal < 0 && !matchOnNull) {
      return new BooleanValueMatcher(false);
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        Object[] dims = holder.getKey().getDims();
        if (dimIndex >= dims.length) {
          return matchOnNull;
        }

        int[] dimsInt = (int[]) dims[dimIndex];
        if (dimsInt == null || dimsInt.length == 0) {
          return matchOnNull;
        }

        for (int i = 0; i < dimsInt.length; i++) {
          if (dimsInt[i] == encodedVal) {
            return true;
          }
        }
        return false;
      }
    };
  }

  @Override
  public ValueMatcher makeIndexingValueMatcher(
      final DruidPredicateFactory predicateFactory,
      final IncrementalIndexStorageAdapter.EntryHolder holder,
      final int dimIndex
  )
  {
    final Predicate<String> predicate = predicateFactory.makeStringPredicate();
    final boolean matchOnNull = predicate.apply(null);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        Object[] dims = holder.getKey().getDims();
        if (dimIndex >= dims.length) {
          return matchOnNull;
        }

        int[] dimsInt = (int[]) dims[dimIndex];
        if (dimsInt == null || dimsInt.length == 0) {
          return matchOnNull;
        }

        for (int i = 0; i < dimsInt.length; i++) {
          String finalDimVal = getActualValue(dimsInt[i], false);
          if (predicate.apply(finalDimVal)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  private void updateSortedLookup()
  {
    if (sortedLookup == null) {
      sortedLookup = dimLookup.sort();
    }
  }

  private String getActualValue(int intermediateValue, boolean idSorted)
  {
    if (idSorted) {
      updateSortedLookup();
      return sortedLookup.getValueFromSortedId(intermediateValue);
    } else {
      return dimLookup.getValue(intermediateValue);

    }
  }

  private int getEncodedValue(String fullValue, boolean idSorted)
  {
    int unsortedId = dimLookup.getId(fullValue);

    if (idSorted) {
      updateSortedLookup();
      return sortedLookup.getSortedIdFromUnsortedId(unsortedId);
    } else {
      return unsortedId;
    }
  }
}
