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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRow;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StringDimensionIndexer implements DimensionIndexer<Integer, int[], String>
{

  @Nullable
  private static String emptyToNullIfNeeded(@Nullable Object o)
  {
    return o != null ? NullHandling.emptyToNullIfNeeded(o.toString()) : null;
  }

  private static final int ABSENT_VALUE_ID = -1;

  private static class DimensionDictionary
  {
    @Nullable
    private String minValue = null;
    @Nullable
    private String maxValue = null;
    private int idForNull = ABSENT_VALUE_ID;

    private final Object2IntMap<String> valueToId = new Object2IntOpenHashMap<>();

    private final List<String> idToValue = new ArrayList<>();
    private final ReentrantReadWriteLock lock;

    public DimensionDictionary()
    {
      this.lock = new ReentrantReadWriteLock();
      valueToId.defaultReturnValue(-1);
    }

    public int getId(@Nullable String value)
    {
      lock.readLock().lock();
      try {
        if (value == null) {
          return idForNull;
        }
        return valueToId.getInt(value);
      }
      finally {
        lock.readLock().unlock();
      }
    }

    @Nullable
    public String getValue(int id)
    {
      lock.readLock().lock();
      try {
        if (id == idForNull) {
          return null;
        }
        return idToValue.get(id);
      }
      finally {
        lock.readLock().unlock();
      }
    }

    public int size()
    {
      lock.readLock().lock();
      try {
        // using idToValue rather than valueToId because the valueToId doesn't account null value, if it is present.
        return idToValue.size();
      }
      finally {
        lock.readLock().unlock();
      }
    }

    public int add(@Nullable String originalValue)
    {
      lock.writeLock().lock();
      try {
        if (originalValue == null) {
          if (idForNull == ABSENT_VALUE_ID) {
            idForNull = idToValue.size();
            idToValue.add(null);
          }
          return idForNull;
        }
        int prev = valueToId.getInt(originalValue);
        if (prev >= 0) {
          return prev;
        }
        final int index = idToValue.size();
        valueToId.put(originalValue, index);
        idToValue.add(originalValue);
        minValue = minValue == null || minValue.compareTo(originalValue) > 0 ? originalValue : minValue;
        maxValue = maxValue == null || maxValue.compareTo(originalValue) < 0 ? originalValue : maxValue;
        return index;
      }
      finally {
        lock.writeLock().unlock();
      }
    }

    public String getMinValue()
    {
      lock.readLock().lock();
      try {
        return minValue;
      }
      finally {
        lock.readLock().unlock();
      }
    }

    public String getMaxValue()
    {
      lock.readLock().lock();
      try {
        return maxValue;
      }
      finally {
        lock.readLock().unlock();
      }
    }

    public SortedDimensionDictionary sort()
    {
      lock.readLock().lock();
      try {
        return new SortedDimensionDictionary(idToValue, idToValue.size());
      }
      finally {
        lock.readLock().unlock();
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
      Object2IntSortedMap<String> sortedMap = new Object2IntRBTreeMap<>(Comparators.naturalNullsFirst());
      for (int id = 0; id < length; id++) {
        String value = idToValue.get(id);
        sortedMap.put(value, id);
      }
      this.sortedVals = Lists.newArrayList(sortedMap.keySet());
      this.idToIndex = new int[length];
      this.indexToId = new int[length];
      int index = 0;
      for (IntIterator iterator = sortedMap.values().iterator(); iterator.hasNext(); ) {
        int id = iterator.nextInt();
        idToIndex[id] = index;
        indexToId[index] = id;
        index++;
      }
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
      return sortedVals.get(index);
    }
  }

  private final DimensionDictionary dimLookup;
  private final MultiValueHandling multiValueHandling;
  private final boolean hasBitmapIndexes;
  private volatile boolean hasMultipleValues = false;
  private volatile boolean isSparse = false;

  @Nullable
  private SortedDimensionDictionary sortedLookup;

  public StringDimensionIndexer(MultiValueHandling multiValueHandling, boolean hasBitmapIndexes)
  {
    this.dimLookup = new DimensionDictionary();
    this.multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;
    this.hasBitmapIndexes = hasBitmapIndexes;
  }

  @Override
  public int[] processRowValsToUnsortedEncodedKeyComponent(@Nullable Object dimValues, boolean reportParseExceptions)
  {
    final int[] encodedDimensionValues;
    final int oldDictSize = dimLookup.size();

    if (dimValues == null) {
      final int nullId = dimLookup.getId(null);
      encodedDimensionValues = nullId == ABSENT_VALUE_ID ? new int[]{dimLookup.add(null)} : new int[]{nullId};
    } else if (dimValues instanceof List) {
      List<Object> dimValuesList = (List<Object>) dimValues;
      if (dimValuesList.isEmpty()) {
        dimLookup.add(null);
        encodedDimensionValues = IntArrays.EMPTY_ARRAY;
      } else if (dimValuesList.size() == 1) {
        encodedDimensionValues = new int[]{dimLookup.add(emptyToNullIfNeeded(dimValuesList.get(0)))};
      } else {
        hasMultipleValues = true;
        final String[] dimensionValues = new String[dimValuesList.size()];
        for (int i = 0; i < dimValuesList.size(); i++) {
          dimensionValues[i] = emptyToNullIfNeeded(dimValuesList.get(i));
        }
        if (multiValueHandling.needSorting()) {
          // Sort multival row by their unencoded values first.
          Arrays.sort(dimensionValues, Comparators.naturalNullsFirst());
        }

        final int[] retVal = new int[dimensionValues.length];

        int prevId = -1;
        int pos = 0;
        for (String dimensionValue : dimensionValues) {
          if (multiValueHandling != MultiValueHandling.SORTED_SET) {
            retVal[pos++] = dimLookup.add(dimensionValue);
            continue;
          }
          int index = dimLookup.add(dimensionValue);
          if (index != prevId) {
            prevId = retVal[pos++] = index;
          }
        }

        encodedDimensionValues = pos == retVal.length ? retVal : Arrays.copyOf(retVal, pos);
      }
    } else {
      encodedDimensionValues = new int[]{dimLookup.add(emptyToNullIfNeeded(dimValues))};
    }

    // If dictionary size has changed, the sorted lookup is no longer valid.
    if (oldDictSize != dimLookup.size()) {
      sortedLookup = null;
    }

    return encodedDimensionValues;
  }

  @Override
  public void setSparseIndexed()
  {
    isSparse = true;
  }

  @Override
  public long estimateEncodedKeyComponentSize(int[] key)
  {
    // string length is being accounted for each time they are referenced, based on dimension handler interface,
    // even though they are stored just once. It may overestimate the size by a bit, but we wanted to leave
    // more buffer to be safe
    long estimatedSize = key.length * Integer.BYTES;
    long totalChars = 0;
    for (int element : key) {
      String val = dimLookup.getValue(element);
      if (val != null) {
        totalChars += val.length();
      }
    }
    estimatedSize += totalChars * Character.BYTES;
    return estimatedSize;
  }

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
  public CloseableIndexed<String> getSortedIndexedValues()
  {
    return new CloseableIndexed<String>()
    {

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
        return id < 0 ? ABSENT_VALUE_ID : getSortedEncodedValueFromUnsorted(id);
      }

      @Override
      public Iterator<String> iterator()
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

    int lenCompareResult = Ints.compare(lhsLen, rhsLen);
    if (lenCompareResult != 0) {
      // if the values don't have the same length, check if we're comparing [] and [null], which are equivalent
      if (lhsLen + rhsLen == 1) {
        int[] longerVal = rhsLen > lhsLen ? rhs : lhs;
        if (longerVal[0] == dimLookup.idForNull) {
          return 0;
        } else {
          //noinspection ArrayEquality -- longerVal is explicitly set to only lhs or rhs
          return longerVal == lhs ? 1 : -1;
        }
      }
    }

    int valsIndex = 0;
    int lenToCompare = Math.min(lhsLen, rhsLen);
    while (valsIndex < lenToCompare) {
      int lhsVal = lhs[valsIndex];
      int rhsVal = rhs[valsIndex];
      if (lhsVal != rhsVal) {
        final String lhsValActual = getActualValue(lhsVal, false);
        final String rhsValActual = getActualValue(rhsVal, false);
        int valueCompareResult = 0;
        if (lhsValActual != null && rhsValActual != null) {
          valueCompareResult = lhsValActual.compareTo(rhsValActual);
        } else if (lhsValActual == null ^ rhsValActual == null) {
          valueCompareResult = lhsValActual == null ? -1 : 1;
        }
        if (valueCompareResult != 0) {
          return valueCompareResult;
        }
      }
      ++valsIndex;
    }

    return lenCompareResult;
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
      final IncrementalIndexRowHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final ExtractionFn extractionFn = spec.getExtractionFn();

    final int dimIndex = desc.getIndex();
    final int maxId = getCardinality();

    class IndexerDimensionSelector implements DimensionSelector, IdLookup
    {
      private final ArrayBasedIndexedInts indexedInts = new ArrayBasedIndexedInts();
      private int[] nullIdIntArray;

      @Override
      public IndexedInts getRow()
      {
        final Object[] dims = currEntry.get().getDims();

        int[] indices;
        if (dimIndex < dims.length) {
          indices = (int[]) dims[dimIndex];
        } else {
          indices = null;
        }

        int[] row = null;
        int rowSize = 0;

        // usually due to currEntry's rowIndex is smaller than the row's rowIndex in which this dim first appears
        if (indices == null || indices.length == 0) {
          if (hasMultipleValues) {
            row = IntArrays.EMPTY_ARRAY;
            rowSize = 0;
          } else {
            final int nullId = getEncodedValue(null, false);
            if (nullId > -1) {
              if (nullIdIntArray == null) {
                nullIdIntArray = new int[]{nullId};
              }
              row = nullIdIntArray;
              rowSize = 1;
            } else {
              // doesn't contain nullId, then empty array is used
              // Choose to use ArrayBasedIndexedInts later, instead of special "empty" IndexedInts, for monomorphism
              row = IntArrays.EMPTY_ARRAY;
              rowSize = 0;
            }
          }
        }

        if (row == null && indices != null && indices.length > 0) {
          row = indices;
          rowSize = indices.length;
        }

        indexedInts.setValues(row, rowSize);
        return indexedInts;
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
                Object[] dims = currEntry.get().getDims();
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

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                // nothing to inspect
              }
            };
          } else {
            return BooleanValueMatcher.of(false);
          }
        } else {
          // Employ caching BitSet optimization
          return makeValueMatcher(Predicates.equalTo(value));
        }
      }

      @Override
      public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
      {
        final BitSet checkedIds = new BitSet(maxId);
        final BitSet matchingIds = new BitSet(maxId);
        final boolean matchNull = predicate.apply(null);

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            Object[] dims = currEntry.get().getDims();
            if (dimIndex >= dims.length) {
              return matchNull;
            }

            int[] dimsInt = (int[]) dims[dimIndex];
            if (dimsInt == null || dimsInt.length == 0) {
              return matchNull;
            }

            for (int id : dimsInt) {
              if (checkedIds.get(id)) {
                if (matchingIds.get(id)) {
                  return true;
                }
              } else {
                final boolean matches = predicate.apply(lookupName(id));
                checkedIds.set(id);
                if (matches) {
                  matchingIds.set(id);
                  return true;
                }
              }
            }
            return false;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // nothing to inspect
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
        if (id >= maxId) {
          throw new ISE("id[%d] >= maxId[%d]", id, maxId);
        }
        final String strValue = getActualValue(id, false);
        return extractionFn == null ? strValue : extractionFn.apply(strValue);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        // name lookup is possible in advance if we got a value for every row (setSparseIndexed was not called on this
        // column) or we've encountered an actual null value and it is present in our dictionary
        return !isSparse || dimLookup.idForNull != ABSENT_VALUE_ID;
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

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Object getObject()
      {
        IncrementalIndexRow key = currEntry.get();
        if (key == null) {
          return null;
        }

        Object[] dims = key.getDims();
        if (dimIndex >= dims.length) {
          return null;
        }

        return convertUnsortedEncodedKeyComponentToActualList((int[]) dims[dimIndex]);
      }

      @SuppressWarnings("deprecation")
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }
    return new IndexerDimensionSelector();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    return makeDimensionSelector(DefaultDimensionSpec.of(desc.getName()), currEntry, desc);
  }

  @Nullable
  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(int[] key)
  {
    if (key == null || key.length == 0) {
      return null;
    }
    if (key.length == 1) {
      return getActualValue(key[0], false);
    } else {
      String[] rowArray = new String[key.length];
      for (int i = 0; i < key.length; i++) {
        String val = getActualValue(key[i], false);
        rowArray[i] = NullHandling.nullToEmptyIfNeeded(val);
      }
      return Arrays.asList(rowArray);
    }
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

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      int[] key,
      int rowNum,
      MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    if (!hasBitmapIndexes) {
      throw new UnsupportedOperationException("This column does not include bitmap indexes");
    }

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

  @Nullable
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
