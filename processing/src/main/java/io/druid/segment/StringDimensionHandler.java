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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.RTree;
import com.metamx.collections.spatial.search.Bound;
import com.metamx.collections.spatial.split.LinearGutmanSplitStrategy;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.io.smoosh.FileSmoosher;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.collections.CombiningIterable;
import io.druid.common.guava.FileOutputSupplier;
import io.druid.common.utils.SerializerUtils;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVSizeIndexedV3Writer;
import io.druid.segment.data.CompressedVSizeIntsIndexedWriter;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIntsWriter;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.data.VSizeIndexedIntsWriter;
import io.druid.segment.data.VSizeIndexedWriter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringDimensionHandler implements DimensionHandler<Integer, String>
{
  private static final Logger log = new Logger(StringDimensionHandler.class);

  private static final ListIndexed EMPTY_STR_DIM_VAL = new ListIndexed<>(Arrays.asList(""), String.class);
  private static final int[] EMPTY_STR_DIM_ARRAY = new int[]{0};

  protected static final Splitter SPLITTER = Splitter.on(",");

  private final String dimensionName;

  public StringDimensionHandler(String dimensionName)
  {
    this.dimensionName = dimensionName;
  }

  @Override
  public Function<Object, String> getValueTypeTransformer()
  {
    return STRING_TRANSFORMER;
  }

  @Override
  public int getEncodedValueSize()
  {
    return Ints.BYTES;
  }

  @Override
  public int getLengthFromArrayObject(Object dimVals)
  {
    return ((int[]) dimVals).length;
  }

  @Override
  public int compareRowboatKey(Object lhsObj, Object rhsObj)
  {
    int[] lhs = (int[]) lhsObj;
    int[] rhs = (int[]) rhsObj;
    int lhsLen = lhs.length;
    int rhsLen = rhs.length;

    int retVal = Ints.compare(lhsLen, rhsLen);

    int valsIndex = 0;
    while (retVal == 0 && valsIndex < lhsLen) {
      retVal = Ints.compare(lhs[valsIndex], rhs[valsIndex]);
      ++valsIndex;
    }
    return retVal;
  }

  @Override
  public int compareEncodedValueAsBytes(byte[] lhs, byte[] rhs)
  {
    int val1 = Ints.fromByteArray(lhs);
    int val2 = Ints.fromByteArray(rhs);
    return Ints.compare(val1, val2);
  }

  @Override
  public List<ByteBuffer> groupByUpdater(
      DimensionSelector selector, ByteBuffer oldKey, Function<ByteBuffer, List<ByteBuffer>> updateFn
  )
  {
    List<ByteBuffer> buffers = null;
    IndexedInts row = selector.getRow();

    if (row == null || row.size() == 0) {
      ByteBuffer newKey = oldKey.duplicate();
      newKey.putInt(-1);
      buffers = updateFn.apply(newKey);
    } else {
      for (int dimValue : row) {
        ByteBuffer newKey = oldKey.duplicate();
        newKey.putInt(dimValue);
        buffers = updateFn.apply(newKey);
      }
    }
    return buffers;
  }

  @Override
  public void addValueToEventFromGroupByKey(ByteBuffer key, DimensionSelector selector, Map<String, Object> event, String outputName)
  {
    int dimVal = key.getInt();
    if (dimVal > -1) {
      event.put(outputName, selector.lookupName(dimVal));
    }
  }

  @Override
  public void addValueToEventFromGroupByKey(ByteBuffer key, DimensionSelector selector, Map<String, Object> event, String outputName, int position)
  {
    int dimVal = key.getInt(position);
    if (dimVal > -1) {
      event.put(outputName, selector.lookupName(dimVal));
    }
  }

  @Override
  public void fishyFunction(ByteBuffer keyBuffer, int bufPosition, GroupByQueryEngine.GroupByDimensionInfo[] dimInfo, int[] stack, IndexedInts[] valuess, int dimIndex, boolean readNewValues)
  {
    final GroupByQueryEngine.GroupByDimensionInfo info = dimInfo[dimIndex];

    if (readNewValues) {
      valuess[dimIndex] = info.selector == null ? EmptyIndexedInts.EMPTY_INDEXED_INTS : info.selector.getRow();
    }

    if (valuess[dimIndex].size() == 0) {
      stack[dimIndex] = 0;
      keyBuffer.putInt(bufPosition, -1);
    } else {
      stack[dimIndex] = 1;
      keyBuffer.putInt(bufPosition, valuess[dimIndex].get(0));
    }
  }

  @Override
  public void fishyFunction2(ByteBuffer keyBuffer, int bufPosition, GroupByQueryEngine.GroupByDimensionInfo[] dimInfo, int[] stack, IndexedInts[] valuess, int dimIndex)
  {
    keyBuffer.putInt(
        bufPosition,
        valuess[dimIndex].get(stack[dimIndex])
    );
  }

  @Override
  public void groupByMergeKeySerde1(
  )
  {
    final int id = addToDictionary(key.getDimensions()[i]);
    if (id < 0) {
      return null;
    }
    keyBuffer.putInt(id);
  }

  @Override
  public Iterable<String> getStringIterableFromSelector(final DimensionSelector selector)
  {
    // This is faster than Iterables.transform() on the IndexedInts row, avoids integer boxing
    return new Iterable<String>()
    {
      @Override
      public Iterator<String> iterator()
      {
        return new Iterator<String>()
        {
          int idx = 0;
          final IndexedInts vals = selector.getRow();

          @Override
          public boolean hasNext()
          {
            return idx < vals.size();
          }

          @Override
          public String next()
          {
            String ret = selector.lookupName(vals.get(idx));
            idx++;
            return ret;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException("remove() is not supported.");
          }
        };
      }
    };
  }

  @Override
  public Object getRowValuesForSelect(DimensionSelector selector)
  {
    Object dimVals;
    final IndexedInts vals = selector.getRow();
    if (vals.size() == 1) {
      dimVals = selector.lookupName(vals.get(0));
    } else {
      List<String> strVals = Lists.newArrayList();
      for (int i = 0; i < vals.size(); ++i) {
        strVals.add(selector.lookupName(vals.get(i)));
      }
      dimVals = strVals;
    }
    return dimVals;
  }

  @Override
  public DimensionIndexer makeIndexer()
  {
    return new StringDimensionIndexer();
  }

  @Override
  public DimensionMergerV9 makeMerger(
      IndexSpec indexSpec,
      File outDir,
      IOPeon ioPeon,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    return new StringDimensionMerger(indexSpec, outDir, ioPeon, capabilities, progress);
  }

  @Override
  public DimensionMergerLegacy makeLegacyMerger(
      IndexSpec indexSpec,
      File outDir,
      IOPeon ioPeon,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    return new StringDimensionMergerLegacy(indexSpec, outDir, ioPeon, capabilities, progress);
  }

  @Override
  public DimensionColumnReader makeColumnReader(Column column)
  {
    return new StringDimensionColumnReader(column);
  }

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
      return String.valueOf(o);
    }
  };

  private static final Comparator<Integer> ENCODED_COMPARATOR = new Comparator<Integer>()
  {
    @Override
    public int compare(Integer o1, Integer o2)
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

  private static final Comparator<String> UNENCODED_COMPARATOR = new Comparator<String>()
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

  public class StringDimensionIndexer implements DimensionIndexer<Integer, String>
  {
    private DimensionDictionary dimLookup;
    private SortedDimensionDictionary sortedLookup;

    public StringDimensionIndexer()
    {
      this.dimLookup = new DimensionDictionary();
    }

    @Override
    public Object processSingleRowValToIndexKey(Object dimVal)
    {
      final int[] encodedDimensionValues;
      int oldDictSize = dimLookup.size();
      try {
        if (dimVal == null) {
          dimLookup.add(null);
          encodedDimensionValues = null;
        } else {
          String transformedVal = STRING_TRANSFORMER.apply(dimVal);
          encodedDimensionValues = new int[]{dimLookup.add(transformedVal)};
        }
      }
      catch (ParseException pe) {
        throw new ParseException(pe, pe.getMessage() + dimensionName);
      }

      // If dictionary size has changed, the sorted lookup is no longer valid.
      if (oldDictSize != dimLookup.size()) {
        sortedLookup = null;
      }

      return encodedDimensionValues;
    }

    @Override
    public Object processRowValsListToIndexKey(Object dimValuesListObj)
    {
      final int[] encodedDimensionValues;
      int oldDictSize = dimLookup.size();
      try {
        if (dimValuesListObj == null) {
          dimLookup.add(null);
          encodedDimensionValues = null;
        } else {
          List<Object> dimValuesList = (List) dimValuesListObj;

          // Sort multival row by their unencoded values first.
          final String[] dimensionValues = new String[dimValuesList.size()];
          for (int i = 0; i < dimValuesList.size(); i++) {
            dimensionValues[i] = STRING_TRANSFORMER.apply(dimValuesList.get(i));
          }
          Arrays.sort(dimensionValues, UNENCODED_COMPARATOR);

          encodedDimensionValues = new int[dimensionValues.length];
          for (int i = 0; i < dimensionValues.length; i++) {
            encodedDimensionValues[i] = dimLookup.add(dimensionValues[i]);
          }
        }
      }
      catch (ParseException pe) {
        throw new ParseException(pe, pe.getMessage() + dimensionName);
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
    public int compareTimeAndDimsKey(Object lhsObj, Object rhsObj)
    {
      int[] lhs = (int[]) lhsObj;
      int[] rhs = (int[]) rhsObj;

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
    public boolean checkTimeAndDimsKeyEqual(Object lhs, Object rhs)
    {
      return Arrays.equals((int[]) lhs, (int[]) rhs);
    }

    @Override
    public int getTimeAndDimsKeyHashcode(Object key)
    {
      return Arrays.hashCode((int[]) key);
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
          List<Integer> valsTmp = null;
          if ((indices == null || indices.length == 0) && nullId > -1) {
            if (nullId < maxId) {
              valsTmp = new ArrayList<>(1);
              valsTmp.add(nullId);
            }
          } else if (indices != null && indices.length > 0) {
            valsTmp = new ArrayList<>(indices.length);
            for (int i = 0; i < indices.length; i++) {
              int id = indices[i];
              if (id < maxId) {
                valsTmp.add(id);
              }
            }
          }

          final List<Integer> vals = valsTmp == null ? Collections.EMPTY_LIST : valsTmp;
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
            public Iterator<Integer> iterator()
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

        @Override
        public ColumnCapabilities getDimCapabilities()
        {
          return desc.getCapabilities();
        }
      };
    }

    @Override
    public Object convertTimeAndDimsKeyToActualArray(Object key)
    {
      int[] dimVals = (int[]) key;
      if (dimVals == null || dimVals.length == 0) {
        return null;
      }
      if (dimVals.length == 1) {
        String val = getActualValue(dimVals[0], false);
        val = Strings.nullToEmpty(val);
        return val;
      } else {
        String[] rowVals = new String[dimVals.length];
        for (int j = 0; j < dimVals.length; j++) {
          String val = getActualValue(dimVals[j], false);
          rowVals[j] = Strings.nullToEmpty(val);
        }
        return rowVals;
      }
    }

    @Override
    public Object convertTimeAndDimsKeyToSortedEncodedArray(Object key)
    {
      int[] dimVals = (int[]) key;
      int[] sortedDimVals = new int[dimVals.length];
      for (int i = 0; i < dimVals.length; ++i) {
        // The encoded values in the TimeAndDims key are not sorted based on their final unencoded values, so need this lookup.
        sortedDimVals[i] = getSortedEncodedValueFromUnsorted(dimVals[i]);
      }
      return  sortedDimVals;
    }

    @Override
    public void fillBitmapsFromTimeAndDimsKey(
        Object key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
    )
    {
      for (int dimValIdx : (int[]) key) {
        if (bitmapIndexes[dimValIdx] == null) {
          bitmapIndexes[dimValIdx] = factory.makeEmptyMutableBitmap();
        }
        bitmapIndexes[dimValIdx].add(rowNum);
      }
    }

    @Override
    public Predicate makeTimeAndDimsKeyValueMatcherPredicate(Comparable matchObj)
    {
      final String matchValue = STRING_TRANSFORMER.apply(matchObj);
      final int encodedVal = getEncodedValue(matchValue, false);
      final boolean matchNull = Strings.isNullOrEmpty(matchValue);

      if(encodedVal < 0 && !matchNull) {
        return null;
      }

      return new Predicate()
      {
        @Override
        public boolean apply(Object dimValsObj)
        {
          int[] dimVals = (int[]) dimValsObj;
          if (dimVals == null || dimVals.length == 0) {
            return matchNull;
          }
          for (int dimVal : dimVals) {
            if (dimVal == encodedVal) {
              return true;
            }
          }
          return false;
        }
      };
    }

    @Override
    public Predicate makeTimeAndDimsKeyValueMatcherPredicate(final Predicate basePredicate)
    {
      final boolean matchNull = basePredicate.apply(null);
      return new Predicate()
      {
        @Override
        public boolean apply(Object dimValsObj)
        {
          int[] dimVals = (int[]) dimValsObj;
          if (dimVals == null || dimVals.length == 0) {
            return matchNull;
          }
          for (int dimVal : dimVals) {
            String finalDimVal = getActualValue(dimVal, false);
            if (basePredicate.apply(finalDimVal)) {
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


  public class StringDimensionMerger implements DimensionMergerV9
  {
    private GenericIndexedWriter<String> dictionaryWriter;
    private GenericIndexedWriter<ImmutableBitmap> bitmapWriter;
    private IndexedIntsWriter encodedValueWriter;
    private ByteBufferWriter<ImmutableRTree> spatialWriter;
    private ArrayList<IntBuffer> dimConversions;
    private int cardinality = 0;
    private boolean convertMissingValues = false;
    private boolean hasNull = false;
    private MutableBitmap nullRowsBitmap;
    private IOPeon ioPeon;
    private int rowCount = 0;
    private ColumnCapabilities capabilities;
    private final File outDir;
    private List<IndexableAdapter> adapters;
    private ProgressIndicator progress;
    private final IndexSpec indexSpec;

    public StringDimensionMerger(
        IndexSpec indexSpec,
        File outDir,
        IOPeon ioPeon,
        ColumnCapabilities capabilities,
        ProgressIndicator progress
    )
    {
      this.indexSpec = indexSpec;
      this.capabilities = capabilities;
      this.outDir = outDir;
      this.ioPeon = ioPeon;
      this.progress = progress;
      nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }

    @Override
    public void mergeValueMetadataAcrossSegments(List<IndexableAdapter> adapters) throws IOException
    {
      boolean dimHasValues = false;
      boolean dimAbsentFromSomeIndex = false;

      long dimStartTime = System.currentTimeMillis();

      this.adapters = adapters;

      dimConversions = Lists.newArrayListWithCapacity(adapters.size());
      for (int i = 0; i < adapters.size(); ++i) {
        dimConversions.add(null);
      }

      int numMergeIndex = 0;
      Indexed<String> dimValueLookup = null;
      Indexed<String>[] dimValueLookups = new Indexed[adapters.size() + 1];
      for (int i = 0; i < adapters.size(); i++) {
        Indexed<String> dimValues = adapters.get(i).getDimValueLookup(dimensionName);
        if (!isNullColumn(dimValues)) {
          dimHasValues = true;
          hasNull |= dimValues.indexOf(null) >= 0;
          dimValueLookups[i] = dimValueLookup = dimValues;
          numMergeIndex++;
        } else {
          dimAbsentFromSomeIndex = true;
        }
      }

      convertMissingValues = dimHasValues && dimAbsentFromSomeIndex;

      /*
       * Ensure the empty str is always in the dictionary if the dimension was missing from one index but
       * has non-null values in another index.
       * This is done so that MMappedIndexRowIterable can convert null columns to empty strings
       * later on, to allow rows from indexes without a particular dimension to merge correctly with
       * rows from indexes with null/empty str values for that dimension.
       */
      if (convertMissingValues && !hasNull) {
        hasNull = true;
        dimValueLookups[adapters.size()] = dimValueLookup = EMPTY_STR_DIM_VAL;
        numMergeIndex++;
      }

      String dictFilename = String.format("%s.dim_values", dimensionName);
      dictionaryWriter = new GenericIndexedWriter<>(
          ioPeon,
          dictFilename,
          GenericIndexed.STRING_STRATEGY
      );
      dictionaryWriter.open();

      cardinality = 0;
      if (numMergeIndex > 1) {
        IndexMerger.DictionaryMergeIterator iterator = new IndexMerger.DictionaryMergeIterator(dimValueLookups, true);

        while (iterator.hasNext()) {
          dictionaryWriter.write(iterator.next());
        }

        for (int i = 0; i < adapters.size(); i++) {
          if (dimValueLookups[i] != null && iterator.needConversion(i)) {
            dimConversions.set(i, iterator.conversions[i]);
          }
        }
        cardinality = iterator.counter;
      } else if (numMergeIndex == 1) {
        for (String value : dimValueLookup) {
          dictionaryWriter.write(value);
        }
        cardinality = dimValueLookup.size();
      }

      log.info(
          "Completed dim[%s] conversions with cardinality[%,d] in %,d millis.",
          dimensionName,
          cardinality,
          System.currentTimeMillis() - dimStartTime
      );
      dictionaryWriter.close();

      if (cardinality == 0) {
        log.info(String.format("Skipping [%s], it is empty!", dimensionName));
      }

      final CompressedObjectStrategy.CompressionStrategy compressionStrategy = indexSpec.getDimensionCompressionStrategy();

      String filenameBase = String.format("%s.forward_dim", dimensionName);
      if (capabilities.hasMultipleValues()) {
        encodedValueWriter = (compressionStrategy != null)
                             ? CompressedVSizeIndexedV3Writer.create(ioPeon, filenameBase, cardinality, compressionStrategy)
                             : new VSizeIndexedWriter(ioPeon, filenameBase, cardinality);
      } else {
        encodedValueWriter = (compressionStrategy != null)
                             ? CompressedVSizeIntsIndexedWriter.create(ioPeon, filenameBase, cardinality, compressionStrategy)
                             : new VSizeIndexedIntsWriter(ioPeon, filenameBase, cardinality);
      }
      encodedValueWriter.open();
    }

    @Override
    public Object convertSegmentRowValuesToMergedRowValues(Object segmentRow, int segmentIndexNumber)
    {
      int[] dimVals = (int[]) segmentRow;
      // For strings, convert missing values to null/empty if conversion flag is set
      // But if bitmap/dictionary is not used, always convert missing to 0
      if (dimVals == null) {
        return convertMissingValues ? EMPTY_STR_DIM_ARRAY : null;
      }

      int[] newDimVals = new int[dimVals.length];
      IntBuffer converter = dimConversions.get(segmentIndexNumber);

      for (int i = 0; i < dimVals.length; i++) {
        if (converter != null) {
          newDimVals[i] = converter.get(dimVals[i]);
        } else {
          newDimVals[i] = dimVals[i];
        }
      }

      return newDimVals;
    }

    @Override
    public void processMergedRow(Object rowValues) throws IOException
    {
      int[] vals = (int[]) rowValues;
      if (vals == null || vals.length == 0) {
        nullRowsBitmap.add(rowCount);
      } else if (hasNull && vals.length == 1 && (vals[0]) == 0) {
        // Dictionary encoded, so it's safe to cast dim value to integer
        // If this dimension has the null/empty str in its dictionary, a row with a single-valued dimension
        // that matches the null/empty str's dictionary ID should also be added to nullRowBitmap.
        nullRowsBitmap.add(rowCount);
      }
      encodedValueWriter.add(vals);
      rowCount++;
    }

    @Override
    public void buildIndexes(List<IntBuffer> segmentRowNumConversions) throws IOException
    {
      long dimStartTime = System.currentTimeMillis();
      final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

      String bmpFilename = String.format("%s.inverted", dimensionName);
      bitmapWriter = new GenericIndexedWriter<>(
          ioPeon,
          bmpFilename,
          indexSpec.getBitmapSerdeFactory().getObjectStrategy()
      );
      bitmapWriter.open();

      // write dim values to one single file because we need to read it
      File dimValueFile = IndexIO.makeDimFile(outDir, dimensionName);
      FileOutputStream fos = new FileOutputStream(dimValueFile);
      ByteStreams.copy(dictionaryWriter.combineStreams(), fos);
      fos.close();

      final MappedByteBuffer dimValsMapped = Files.map(dimValueFile);
      Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, GenericIndexed.STRING_STRATEGY);
      BitmapFactory bmpFactory = bitmapSerdeFactory.getBitmapFactory();

      RTree tree = null;
      boolean hasSpatial = capabilities.hasSpatialIndexes();
      if(hasSpatial) {
        BitmapFactory bitmapFactory = indexSpec.getBitmapSerdeFactory().getBitmapFactory();
        spatialWriter = new ByteBufferWriter<>(
            ioPeon,
            String.format("%s.spatial", dimensionName),
            new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapFactory)
        );
        spatialWriter.open();
        tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bmpFactory), bmpFactory);
      }

      IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimensionName);

      //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
      for (int dictId = 0; dictId < dimVals.size(); dictId++) {
        progress.progress();
        List<Iterable<Integer>> convertedInverteds = Lists.newArrayListWithCapacity(adapters.size());
        for (int j = 0; j < adapters.size(); ++j) {
          int seekedDictId = dictIdSeeker[j].seek(dictId);
          if (seekedDictId != IndexSeeker.NOT_EXIST) {
            convertedInverteds.add(
                new ConvertingIndexedInts(
                    adapters.get(j).getBitmapIndex(dimensionName, seekedDictId), segmentRowNumConversions.get(j)
                )
            );
          }
        }

        MutableBitmap bitset = bmpFactory.makeEmptyMutableBitmap();
        for (Integer row : CombiningIterable.createSplatted(
            convertedInverteds,
            Ordering.<Integer>natural().nullsFirst()
        )) {
          if (row != IndexMerger.INVALID_ROW) {
            bitset.add(row);
          }
        }

        ImmutableBitmap bitmapToWrite = bitmapSerdeFactory.getBitmapFactory().makeImmutableBitmap(bitset);
        if ((dictId == 0) && (Iterables.getFirst(dimVals, "") == null)) {
          bitmapToWrite = bmpFactory.makeImmutableBitmap(nullRowsBitmap).union(bitmapToWrite);
        }
        bitmapWriter.write(bitmapToWrite);

        if (hasSpatial) {
          String dimVal = dimVals.get(dictId);
          if (dimVal != null) {
            List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
            float[] coords = new float[stringCoords.size()];
            for (int j = 0; j < coords.length; j++) {
              coords[j] = Float.valueOf(stringCoords.get(j));
            }
            tree.insert(coords, bitset);
          }
        }
      }

      if (hasSpatial) {
        spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));
        spatialWriter.close();
      }

      log.info(
          "Completed dim[%s] inverted with cardinality[%,d] in %,d millis.",
          dimensionName,
          dimVals.size(),
          System.currentTimeMillis() - dimStartTime
      );

      bitmapWriter.close();
      encodedValueWriter.close();
    }

    @Override
    public boolean canSkip()
    {
      return cardinality == 0;
    }

    @Override
    public ColumnDescriptor makeColumnDescriptor()
    {
      // Now write everything
      boolean hasMultiValue = capabilities.hasMultipleValues();
      final CompressedObjectStrategy.CompressionStrategy compressionStrategy = indexSpec.getDimensionCompressionStrategy();
      final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
      builder.setValueType(ValueType.STRING);
      builder.setHasMultipleValues(hasMultiValue);
      final DictionaryEncodedColumnPartSerde.SerializerBuilder partBuilder = DictionaryEncodedColumnPartSerde
          .serializerBuilder()
          .withDictionary(dictionaryWriter)
          .withValue(encodedValueWriter, hasMultiValue, compressionStrategy != null)
          .withBitmapSerdeFactory(bitmapSerdeFactory)
          .withBitmapIndex(bitmapWriter)
          .withSpatialIndex(spatialWriter)
          .withByteOrder(IndexIO.BYTE_ORDER);
      final ColumnDescriptor serdeficator = builder
          .addSerde(partBuilder.build())
          .build();

      //log.info("Completed dimension column[%s] in %,d millis.", dimensionName, System.currentTimeMillis() - dimStartTime);

      return serdeficator;
    }

    private boolean isNullColumn(Iterable<String> dimValues)
    {
      if (dimValues == null) {
        return true;
      }
      for (String val : dimValues) {
        if (val != null) {
          return false;
        }
      }
      return true;
    }

    private IndexSeeker[] toIndexSeekers(
        List<IndexableAdapter> adapters,
        ArrayList<IntBuffer> dimConversions,
        String dimension
    )
    {
      IndexSeeker[] seekers = new IndexSeeker[adapters.size()];
      for (int i = 0; i < adapters.size(); i++) {
        IntBuffer dimConversion = dimConversions.get(i);
        if (dimConversion != null) {
          seekers[i] = new IndexSeekerWithConversion((IntBuffer) dimConversion.asReadOnlyBuffer().rewind());
        } else {
          Indexed<String> dimValueLookup = adapters.get(i).getDimValueLookup(dimension);
          seekers[i] = new IndexSeekerWithoutConversion(dimValueLookup == null ? 0 : dimValueLookup.size());
        }
      }
      return seekers;
    }
  }

  public class StringDimensionMergerLegacy implements DimensionMergerLegacy
  {
    private GenericIndexedWriter<String> dictionaryWriter;
    private GenericIndexedWriter<ImmutableBitmap> bitmapWriter;
    private VSizeIndexedWriter encodedValueWriterV8;
    private ByteBufferWriter<ImmutableRTree> spatialWriter;
    private ArrayList<IntBuffer> dimConversions;
    private int cardinality = 0;
    private boolean convertMissingValues = false;
    private boolean hasNull = false;
    private MutableBitmap nullRowsBitmap;
    private IOPeon ioPeon;
    private IOPeon spatialIoPeon;
    private int rowCount = 0;
    private ColumnCapabilities capabilities;
    private List<IndexableAdapter> adapters;
    private ProgressIndicator progress;
    private final IndexSpec indexSpec;

    private File dictionaryFile;

    public StringDimensionMergerLegacy(
        IndexSpec indexSpec,
        File outDir,
        IOPeon ioPeon,
        ColumnCapabilities capabilities,
        ProgressIndicator progress
    )
    {
      this.indexSpec = indexSpec;
      this.capabilities = capabilities;
      this.ioPeon = ioPeon;
      this.progress = progress;
      nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }

    @Override
    public void mergeValueMetadataAcrossSegments(List<IndexableAdapter> adapters) throws IOException
    {
      boolean dimHasValues = false;
      boolean dimAbsentFromSomeIndex = false;

      long dimStartTime = System.currentTimeMillis();

      this.adapters = adapters;

      dimConversions = Lists.newArrayListWithCapacity(adapters.size());
      for (int i = 0; i < adapters.size(); ++i) {
        dimConversions.add(null);
      }

      int numMergeIndex = 0;
      Indexed<String> dimValueLookup = null;
      Indexed<String>[] dimValueLookups = new Indexed[adapters.size() + 1];
      for (int i = 0; i < adapters.size(); i++) {
        Indexed<String> dimValues = adapters.get(i).getDimValueLookup(dimensionName);
        if (!isNullColumn(dimValues)) {
          dimHasValues = true;
          hasNull |= dimValues.indexOf(null) >= 0;
          dimValueLookups[i] = dimValueLookup = dimValues;
          numMergeIndex++;
        } else {
          dimAbsentFromSomeIndex = true;
        }
      }

      convertMissingValues = dimHasValues && dimAbsentFromSomeIndex;

      /*
       * Ensure the empty str is always in the dictionary if the dimension was missing from one index but
       * has non-null values in another index.
       * This is done so that MMappedIndexRowIterable can convert null columns to empty strings
       * later on, to allow rows from indexes without a particular dimension to merge correctly with
       * rows from indexes with null/empty str values for that dimension.
       */
      if (convertMissingValues && !hasNull) {
        hasNull = true;
        dimValueLookups[adapters.size()] = dimValueLookup = EMPTY_STR_DIM_VAL;
        numMergeIndex++;
      }

      String dictFilename = String.format("%s.dim_values", dimensionName);
      dictionaryWriter = new GenericIndexedWriter<>(
          ioPeon,
          dictFilename,
          GenericIndexed.STRING_STRATEGY
      );
      dictionaryWriter.open();

      cardinality = 0;
      if (numMergeIndex > 1) {
        IndexMerger.DictionaryMergeIterator iterator = new IndexMerger.DictionaryMergeIterator(dimValueLookups, true);

        while (iterator.hasNext()) {
          dictionaryWriter.write(iterator.next());
        }

        for (int i = 0; i < adapters.size(); i++) {
          if (dimValueLookups[i] != null && iterator.needConversion(i)) {
            dimConversions.set(i, iterator.conversions[i]);
          }
        }
        cardinality = iterator.counter;
      } else if (numMergeIndex == 1) {
        for (String value : dimValueLookup) {
          dictionaryWriter.write(value);
        }
        cardinality = dimValueLookup.size();
      }

      log.info(
          "Completed dim[%s] conversions with cardinality[%,d] in %,d millis.",
          dimensionName,
          cardinality,
          System.currentTimeMillis() - dimStartTime
      );
      dictionaryWriter.close();

      encodedValueWriterV8 = new VSizeIndexedWriter(ioPeon, dimensionName, cardinality);
      encodedValueWriterV8.open();
    }

    @Override
    public Object convertSegmentRowValuesToMergedRowValues(Object segmentRow, int segmentIndexNumber)
    {
      int[] dimVals = (int[]) segmentRow;
      // For strings, convert missing values to null/empty if conversion flag is set
      // But if bitmap/dictionary is not used, always convert missing to 0
      if (dimVals == null) {
        return convertMissingValues ? EMPTY_STR_DIM_ARRAY : null;
      }

      int[] newDimVals = new int[dimVals.length];
      IntBuffer converter = dimConversions.get(segmentIndexNumber);

      for (int i = 0; i < dimVals.length; i++) {
        if (converter != null) {
          newDimVals[i] = converter.get(dimVals[i]);
        } else {
          newDimVals[i] = dimVals[i];
        }
      }

      return newDimVals;
    }

    @Override
    public void processMergedRow(Object rowValues) throws IOException
    {
      int[] vals = (int[]) rowValues;
      if (vals == null || vals.length == 0) {
        nullRowsBitmap.add(rowCount);
      } else if (hasNull && vals.length == 1 && (vals[0]) == 0) {
        // Dictionary encoded, so it's safe to cast dim value to integer
        // If this dimension has the null/empty str in its dictionary, a row with a single-valued dimension
        // that matches the null/empty str's dictionary ID should also be added to nullRowBitmap.
        nullRowsBitmap.add(rowCount);
      }
      List<Integer> listToWrite = (vals == null)
                                  ? null
                                  : Ints.asList(vals);
      encodedValueWriterV8.add(listToWrite);
      rowCount++;
    }

    @Override
    public void buildIndexes(List<IntBuffer> segmentRowNumConversions) throws IOException
    {
      final SerializerUtils serializerUtils = new SerializerUtils();
      long dimStartTime = System.currentTimeMillis();

      String bmpFilename = String.format("%s.inverted", dimensionName);
      bitmapWriter = new GenericIndexedWriter<>(
          ioPeon,
          bmpFilename,
          indexSpec.getBitmapSerdeFactory().getObjectStrategy()
      );
      bitmapWriter.open();

      final MappedByteBuffer dimValsMapped = Files.map(dictionaryFile);

      if (!dimensionName.equals(serializerUtils.readString(dimValsMapped))) {
        throw new ISE("dimensions[%s] didn't equate!?  This is a major WTF moment.", dimensionName);
      }
      Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, GenericIndexed.STRING_STRATEGY);
      log.info("Starting dimension[%s] with cardinality[%,d]", dimensionName, dimVals.size());

      final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
      final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();

      RTree tree = null;
      spatialWriter = null;
      boolean hasSpatial = capabilities.hasSpatialIndexes();
      spatialIoPeon = new TmpFileIOPeon();
      if (hasSpatial) {
        BitmapFactory bmpFactory = bitmapSerdeFactory.getBitmapFactory();
        String spatialFilename = String.format("%s.spatial", dimensionName);
        spatialWriter = new ByteBufferWriter<ImmutableRTree>(
            spatialIoPeon, spatialFilename, new IndexedRTree.ImmutableRTreeObjectStrategy(bmpFactory)
        );
        spatialWriter.open();
        tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bitmapFactory), bitmapFactory);
      }

      IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimensionName);

      //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
      for (int dictId = 0; dictId < dimVals.size(); dictId++) {
        progress.progress();
        List<Iterable<Integer>> convertedInverteds = Lists.newArrayListWithCapacity(adapters.size());
        for (int j = 0; j < adapters.size(); ++j) {
          int seekedDictId = dictIdSeeker[j].seek(dictId);
          if (seekedDictId != IndexSeeker.NOT_EXIST) {
            convertedInverteds.add(
                new ConvertingIndexedInts(
                    adapters.get(j).getBitmapIndex(dimensionName, seekedDictId), segmentRowNumConversions.get(j)
                )
            );
          }
        }

        MutableBitmap bitset = bitmapSerdeFactory.getBitmapFactory().makeEmptyMutableBitmap();
        for (Integer row : CombiningIterable.createSplatted(
            convertedInverteds,
            Ordering.<Integer>natural().nullsFirst()
        )) {
          if (row != IndexMerger.INVALID_ROW) {
            bitset.add(row);
          }
        }
        if ((dictId == 0) && (Iterables.getFirst(dimVals, "") == null)) {
          bitset.or(nullRowsBitmap);
        }

        bitmapWriter.write(
            bitmapSerdeFactory.getBitmapFactory().makeImmutableBitmap(bitset)
        );

        if (hasSpatial) {
          String dimVal = dimVals.get(dictId);
          if (dimVal != null) {
            List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
            float[] coords = new float[stringCoords.size()];
            for (int j = 0; j < coords.length; j++) {
              coords[j] = Float.valueOf(stringCoords.get(j));
            }
            tree.insert(coords, bitset);
          }
        }
      }


      log.info("Completed dimension[%s] in %,d millis.", dimensionName, System.currentTimeMillis() - dimStartTime);

      if (hasSpatial) {
        spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));

      }
    }

    @Override
    public boolean canSkip()
    {
      return cardinality == 0;
    }

    @Override
    public void writeValueMetadataToFile(FileOutputSupplier valueEncodingFile) throws IOException
    {
      final SerializerUtils serializerUtils = new SerializerUtils();

      dictionaryWriter.close();
      serializerUtils.writeString(valueEncodingFile, dimensionName);
      ByteStreams.copy(dictionaryWriter.combineStreams(), valueEncodingFile);

      // save this File reference, we will read from it later when building bitmap/spatial indexes
      dictionaryFile = valueEncodingFile.getFile();
    }

    @Override
    public void writeRowValuesToFile(FileOutputSupplier rowValueFile) throws IOException
    {
      encodedValueWriterV8.close();
      ByteStreams.copy(encodedValueWriterV8.combineStreams(), rowValueFile);
    }

    @Override
    public void writeIndexesToFiles(
        OutputSupplier<FileOutputStream>  invertedIndexFile,
        OutputSupplier<FileOutputStream>  spatialIndexFile
    ) throws IOException
    {
      final SerializerUtils serializerUtils = new SerializerUtils();

      bitmapWriter.close();
      serializerUtils.writeString(invertedIndexFile, dimensionName);
      ByteStreams.copy(bitmapWriter.combineStreams(), invertedIndexFile);


      if (capabilities.hasSpatialIndexes()) {
        spatialWriter.close();
        serializerUtils.writeString(spatialIndexFile, dimensionName);
        ByteStreams.copy(spatialWriter.combineStreams(), spatialIndexFile);
        spatialIoPeon.cleanup();
      }
    }

    private boolean isNullColumn(Iterable<String> dimValues)
    {
      if (dimValues == null) {
        return true;
      }
      for (String val : dimValues) {
        if (val != null) {
          return false;
        }
      }
      return true;
    }

    private IndexSeeker[] toIndexSeekers(
        List<IndexableAdapter> adapters,
        ArrayList<IntBuffer> dimConversions,
        String dimension
    )
    {
      IndexSeeker[] seekers = new IndexSeeker[adapters.size()];
      for (int i = 0; i < adapters.size(); i++) {
        IntBuffer dimConversion = dimConversions.get(i);
        if (dimConversion != null) {
          seekers[i] = new IndexSeekerWithConversion((IntBuffer) dimConversion.asReadOnlyBuffer().rewind());
        } else {
          Indexed<String> dimValueLookup = adapters.get(i).getDimValueLookup(dimension);
          seekers[i] = new IndexSeekerWithoutConversion(dimValueLookup == null ? 0 : dimValueLookup.size());
        }
      }
      return seekers;
    }
  }

  private interface IndexSeeker
  {
    int NOT_EXIST = -1;
    int NOT_INIT = -1;

    int seek(int dictId);
  }

  private class IndexSeekerWithoutConversion implements IndexSeeker
  {
    private final int limit;

    public IndexSeekerWithoutConversion(int limit)
    {
      this.limit = limit;
    }

    @Override
    public int seek(int dictId)
    {
      return dictId < limit ? dictId : NOT_EXIST;
    }
  }

  /**
   * Get old dictId from new dictId, and only support access in order
   */
  private class IndexSeekerWithConversion implements IndexSeeker
  {
    private final IntBuffer dimConversions;
    private int currIndex;
    private int currVal;
    private int lastVal;

    IndexSeekerWithConversion(IntBuffer dimConversions)
    {
      this.dimConversions = dimConversions;
      this.currIndex = 0;
      this.currVal = NOT_INIT;
      this.lastVal = NOT_INIT;
    }

    public int seek(int dictId)
    {
      if (dimConversions == null) {
        return NOT_EXIST;
      }
      if (lastVal != NOT_INIT) {
        if (dictId <= lastVal) {
          throw new ISE(
              "Value dictId[%d] is less than the last value dictId[%d] I have, cannot be.",
              dictId, lastVal
          );
        }
        return NOT_EXIST;
      }
      if (currVal == NOT_INIT) {
        currVal = dimConversions.get();
      }
      if (currVal == dictId) {
        int ret = currIndex;
        ++currIndex;
        if (dimConversions.hasRemaining()) {
          currVal = dimConversions.get();
        } else {
          lastVal = dictId;
        }
        return ret;
      } else if (currVal < dictId) {
        throw new ISE(
            "Skipped currValue dictId[%d], currIndex[%d]; incoming value dictId[%d]",
            currVal, currIndex, dictId
        );
      } else {
        return NOT_EXIST;
      }
    }
  }

  public static class ConvertingIndexedInts implements Iterable<Integer>
  {
    private final IndexedInts baseIndex;
    private final IntBuffer conversionBuffer;

    public ConvertingIndexedInts(
        IndexedInts baseIndex,
        IntBuffer conversionBuffer
    )
    {
      this.baseIndex = baseIndex;
      this.conversionBuffer = conversionBuffer;
    }

    public int size()
    {
      return baseIndex.size();
    }

    public int get(int index)
    {
      return conversionBuffer.get(baseIndex.get(index));
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return Iterators.transform(
          baseIndex.iterator(),
          new Function<Integer, Integer>()
          {
            @Override
            public Integer apply(@Nullable Integer input)
            {
              return conversionBuffer.get(input);
            }
          }
      );
    }
  }

  public class StringDimensionColumnReader implements DimensionColumnReader<Integer, String>
  {
    private final Column column;
    public StringDimensionColumnReader(Column column)
    {
      this.column = column;
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String value)
    {
      int idx = column.getBitmapIndex().getIndex(value);
      return column.getBitmapIndex().getBitmap(idx);
    }

    @Override
    public String getMinValue()
    {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(0) : null;
    }

    @Override
    public String getMaxValue()
    {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(bitmap.getCardinality() - 1) : null;
    }

    @Override
    public ColumnCapabilities getCapabilities()
    {
      return column.getCapabilities();
    }

    @Override
    public DimensionSelector makeDimensionSelector(
        final DimensionSpec dimensionSpec,
        final QueryableIndexStorageAdapter.CursorOffsetHolder cursorOffsetHolder,
        final Map<String, Closeable> dimensionColumnCache
    )
    {
      final String dimension = dimensionSpec.getDimension();
      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();
      final ColumnCapabilities capabilities = column.getCapabilities();
      DictionaryEncodedColumn cachedColumn = (DictionaryEncodedColumn) dimensionColumnCache.get(dimension);
      if (cachedColumn == null) {
        cachedColumn = column.getDictionaryEncoding();
        dimensionColumnCache.put(dimension, cachedColumn);
      }

      final DictionaryEncodedColumn column = cachedColumn;

      if (column == null) {
        throw new IllegalStateException("Column was null!");
      } else if (capabilities.hasMultipleValues()) {
        return new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            IndexedInts vals = column.getMultiValueRow(cursorOffsetHolder.get().getOffset());
            return vals;
          }

          @Override
          public int getValueCardinality()
          {
            return column.getCardinality();
          }

          @Override
          public String lookupName(int id)
          {
            final String value = column.lookupName(id);
            return extractionFn == null ?
                   value :
                   extractionFn.apply(value);
          }

          @Override
          public int lookupId(String name)
          {
            if (extractionFn != null) {
              throw new UnsupportedOperationException(
                  "cannot perform lookup when applying an extraction function"
              );
            }
            return column.lookupId(name);
          }

          @Override
          public ColumnCapabilities getDimCapabilities()
          {
            return capabilities;
          }
        };
      } else {
        return new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            // using an anonymous class is faster than creating a class that stores a copy of the value
            IndexedInts vals = new IndexedInts()
            {
              @Override
              public int size()
              {
                return 1;
              }

              @Override
              public int get(int index)
              {
                return column.getSingleValueRow(cursorOffsetHolder.get().getOffset());
              }

              @Override
              public Iterator<Integer> iterator()
              {
                return Iterators.singletonIterator(column.getSingleValueRow(cursorOffsetHolder.get().getOffset()));
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
            return vals;
          }

          @Override
          public int getValueCardinality()
          {
            return column.getCardinality();
          }

          @Override
          public String lookupName(int id)
          {
            final String value = column.lookupName(id);
            return extractionFn == null ? value : extractionFn.apply(value);
          }

          @Override
          public int lookupId(String name)
          {
            if (extractionFn != null) {
              throw new UnsupportedOperationException(
                  "cannot perform lookup when applying an extraction function"
              );
            }
            return column.lookupId(name);
          }

          @Override
          public ColumnCapabilities getDimCapabilities()
          {
            return capabilities;
          }
        };
      }
    }

    @Override
    public Indexed<String> getSortedIndexedValues()
    {
      final DictionaryEncodedColumn dictionary = column.getDictionaryEncoding();
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
          return dictionary.getCardinality();
        }

        @Override
        public String get(int index)
        {
          return dictionary.lookupName(index);
        }

        @Override
        public int indexOf(String value)
        {
          return dictionary.lookupId(value);
        }

        @Override
        public Iterator<String> iterator()
        {
          return IndexedIterable.create(this).iterator();
        }
      };
    }
  }
}
