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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import it.unimi.dsi.fastutil.ints.IntArrays;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringDimensionIndexer implements DimensionIndexer<Integer, int[], String>
{
  public static final Function<Object, String> STRING_TRANSFORMER = new Function<Object, String>()
  {
    @Override
    public String apply(final Object o)
    {
      if (o == null) {
        return null;
      }
      if (o instanceof String) {
        return (String) o;
      }
      return o.toString();
    }
  };

  public static final Comparator<String> UNENCODED_COMPARATOR = new Comparator<String>()
  {
    @Override
    public int compare(String o1, String o2)
    {
      if (o1 == null) {
        return o2 == null ? 0 : -1;
      }
      if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    }
  };

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
      synchronized (lock) {
        return minValue;
      }
    }

    public String getMaxValue()
    {
      synchronized (lock) {
        return maxValue;
      }
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

  private final DimensionDictionary dimLookup;
  private final MultiValueHandling multiValueHandling;
  private SortedDimensionDictionary sortedLookup;

  public StringDimensionIndexer(MultiValueHandling multiValueHandling)
  {
    this.dimLookup = new DimensionDictionary();
    this.multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.STRING;
  }

  @Override
  public int[] processRowValsToUnsortedEncodedKeyComponent(Object dimValues)
  {
    final int[] encodedDimensionValues;
    final int oldDictSize = dimLookup.size();

    if (dimValues == null) {
      dimLookup.add(null);
      encodedDimensionValues = null;
    } else if (dimValues instanceof List) {
      List<Object> dimValuesList = (List) dimValues;
      if (dimValuesList.size() == 1) {
        encodedDimensionValues = new int[]{dimLookup.add(STRING_TRANSFORMER.apply(dimValuesList.get(0)))};
      } else {
        final String[] dimensionValues = new String[dimValuesList.size()];
        for (int i = 0; i < dimValuesList.size(); i++) {
          dimensionValues[i] = STRING_TRANSFORMER.apply(dimValuesList.get(i));
        }
        if (multiValueHandling.needSorting()) {
          // Sort multival row by their unencoded values first.
          Arrays.sort(dimensionValues, UNENCODED_COMPARATOR);
        }

        final int[] retVal = new int[dimensionValues.length];

        int prevId = -1;
        int pos = 0;
        for (int i = 0; i < dimensionValues.length; i++) {
          if (multiValueHandling != MultiValueHandling.SORTED_SET) {
            retVal[pos++] = dimLookup.add(dimensionValues[i]);
            continue;
          }
          int index = dimLookup.add(dimensionValues[i]);
          if (index != prevId) {
            prevId = retVal[pos++] = index;
          }
        }

        encodedDimensionValues = pos == retVal.length ? retVal : Arrays.copyOf(retVal, pos);
      }
    } else {
      encodedDimensionValues = new int[]{dimLookup.add(STRING_TRANSFORMER.apply(dimValues))};
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
    return sortedLookup().getSortedIdFromUnsortedId(unsortedIntermediateValue);
  }

  @Override
  public Integer getUnsortedEncodedValueFromSorted(Integer sortedIntermediateValue)
  {
    return sortedLookup().getUnsortedIdFromSortedId(sortedIntermediateValue);
  }

  @Override
  public Indexed<String> getSortedIndexedValues()
  {
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
  public int compareUnsortedEncodedKeyComponents(int[] lhs, int[] rhs)
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
  public boolean checkUnsortedEncodedKeyComponentsEqual(int[] lhs, int[] rhs)
  {
    return Arrays.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(int[] key)
  {
    return Arrays.hashCode(key);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      final DimensionSpec spec,
      final IncrementalIndexStorageAdapter.EntryHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final ExtractionFn extractionFn = spec.getExtractionFn();

    final int dimIndex = desc.getIndex();
    final int maxId = getCardinality();

    class IndexerDimensionSelector implements DimensionSelector, IdLookup
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

        int[] row = null;
        int rowSize = 0;
        if (indices == null || indices.length == 0) {
          final int nullId = getEncodedValue(null, false);
          if (nullId > -1) {
            if (nullId < maxId) {
              row = new int[] {nullId};
              rowSize = 1;
            } else {
              // Choose to use ArrayBasedIndexedInts later, instead of EmptyIndexedInts, for monomorphism
              row = IntArrays.EMPTY_ARRAY;
              rowSize = 0;
            }
          }
        }

        if (row == null && indices != null && indices.length > 0) {
          row = new int[indices.length];
          for (int id : indices) {
            if (id < maxId) {
              row[rowSize++] = id;
            }
          }
        }

        return ArrayBasedIndexedInts.of(row, rowSize);
      }

      @Override
      public ValueMatcher makeValueMatcher(final String value)
      {
        if (extractionFn == null) {
          final int valueId = lookupId(value);
          if (valueId >= 0 || value == null) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                Object[] dims = currEntry.getKey().getDims();
                if (dimIndex >= dims.length) {
                  return value == null;
                }

                int[] dimsInt = (int[]) dims[dimIndex];
                if (dimsInt == null || dimsInt.length == 0) {
                  return value == null;
                }

                for (int id : dimsInt) {
                  if (id == valueId) {
                    return true;
                  }
                }
                return false;
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
      public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
      {
        final BitSet predicateMatchingValueIds = DimensionSelectorUtils.makePredicateMatchingSet(this, predicate);
        final boolean matchNull = predicate.apply(null);
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            Object[] dims = currEntry.getKey().getDims();
            if (dimIndex >= dims.length) {
              return matchNull;
            }

            int[] dimsInt = (int[]) dims[dimIndex];
            if (dimsInt == null || dimsInt.length == 0) {
              return matchNull;
            }

            for (int id : dimsInt) {
              if (predicateMatchingValueIds.get(id)) {
                return true;
              }
            }
            return false;
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
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return extractionFn == null ? this : null;
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

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("currEntry", currEntry);
      }
    }
    return new IndexerDimensionSelector();
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
     return ZeroLongColumnSelector.instance();
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    return ZeroFloatColumnSelector.instance();
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(
      final DimensionSpec spec,
      final IncrementalIndexStorageAdapter.EntryHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final ExtractionFn extractionFn = spec.getExtractionFn();
    final int dimIndex = desc.getIndex();

    class StringIndexerObjectColumnSelector implements ObjectColumnSelector<String>
    {
      @Override
      public Class<String> classOfObject()
      {
        return String.class;
      }

      @Override
      public String get()
      {
        final Object[] dims = currEntry.getKey().getDims();

        int[] indices;
        if (dimIndex < dims.length) {
          indices = (int[]) dims[dimIndex];
          if (indices.length > 1) {
            throw new UnsupportedOperationException(
                "makeObjectColumnSelector does not support multi-value columns."
            );
          }
        } else {
          indices = null;
        }

        if (indices == null || indices.length == 0) {
          return extractionFn.apply(null);
        }

        final String strValue = getActualValue(indices[0], false);
        return extractionFn == null ? strValue : extractionFn.apply(strValue);
      }
    }

    return new StringIndexerObjectColumnSelector();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualArrayOrList(int[] key, boolean asList)
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
  public int[] convertUnsortedEncodedKeyComponentToSortedEncodedKeyComponent(int[] key)
  {
    int[] sortedDimVals = new int[key.length];
    for (int i = 0; i < key.length; ++i) {
      // The encoded values in the TimeAndDims key are not sorted based on their final unencoded values, so need this lookup.
      sortedDimVals[i] = getSortedEncodedValueFromUnsorted(key[i]);
    }
    return sortedDimVals;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
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

  private SortedDimensionDictionary sortedLookup()
  {
    return sortedLookup == null ? sortedLookup = dimLookup.sort() : sortedLookup;
  }

  private String getActualValue(int intermediateValue, boolean idSorted)
  {
    if (idSorted) {
      return sortedLookup().getValueFromSortedId(intermediateValue);
    } else {
      return dimLookup.getValue(intermediateValue);

    }
  }

  private int getEncodedValue(String fullValue, boolean idSorted)
  {
    int unsortedId = dimLookup.getId(fullValue);

    if (idSorted) {
      return sortedLookup().getSortedIdFromUnsortedId(unsortedId);
    } else {
      return unsortedId;
    }
  }
}
