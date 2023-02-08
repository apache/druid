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
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexedWriter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.nested.CompressedNestedDataComplexColumn;
import org.apache.druid.segment.nested.GlobalDictionaryIdLookup;
import org.apache.druid.segment.nested.GlobalDictionarySortedCollector;
import org.apache.druid.segment.nested.NestedDataColumnSerializer;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedLiteralTypeInfo;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

public class NestedDataColumnMerger implements DimensionMergerV9
{
  private static final Logger log = new Logger(NestedDataColumnMerger.class);

  public static final Comparator<PeekingIterator<String>> STRING_MERGING_COMPARATOR =
      SimpleDictionaryMergingIterator.makePeekingComparator();
  public static final Comparator<PeekingIterator<Long>> LONG_MERGING_COMPARATOR =
      SimpleDictionaryMergingIterator.makePeekingComparator();
  public static final Comparator<PeekingIterator<Double>> DOUBLE_MERGING_COMPARATOR =
      SimpleDictionaryMergingIterator.makePeekingComparator();

  private final String name;
  private final IndexSpec indexSpec;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final ProgressIndicator progressIndicator;
  private final Closer closer;

  private ColumnDescriptor.Builder descriptorBuilder;
  private GenericColumnSerializer<?> serializer;

  public NestedDataColumnMerger(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ProgressIndicator progressIndicator,
      Closer closer
  )
  {

    this.name = name;
    this.indexSpec = indexSpec;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.progressIndicator = progressIndicator;
    this.closer = closer;
  }

  @Override
  public void writeMergedValueDictionary(List<IndexableAdapter> adapters) throws IOException
  {
    try {
      long dimStartTime = System.currentTimeMillis();

      int numMergeIndex = 0;
      GlobalDictionarySortedCollector sortedLookup = null;
      final Indexed[] sortedLookups = new Indexed[adapters.size()];
      final Indexed[] sortedLongLookups = new Indexed[adapters.size()];
      final Indexed[] sortedDoubleLookups = new Indexed[adapters.size()];
      final Iterable<Object[]>[] sortedArrayLookups = new Iterable[adapters.size()];

      final SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> mergedFields = new TreeMap<>();

      for (int i = 0; i < adapters.size(); i++) {
        final IndexableAdapter adapter = adapters.get(i);
        final GlobalDictionarySortedCollector dimValues;
        if (adapter instanceof IncrementalIndexAdapter) {
          dimValues = getSortedIndexFromIncrementalAdapter((IncrementalIndexAdapter) adapter, mergedFields);
        } else if (adapter instanceof QueryableIndexIndexableAdapter) {
          dimValues = getSortedIndexesFromQueryableAdapter((QueryableIndexIndexableAdapter) adapter, mergedFields);
        } else {
          throw new ISE("Unable to merge columns of unsupported adapter %s", adapter.getClass());
        }

        boolean allNulls = dimValues == null || allNull(dimValues.getSortedStrings()) &&
                                                allNull(dimValues.getSortedLongs()) &&
                                                allNull(dimValues.getSortedDoubles()) &&
                                                dimValues.getArrayCardinality() == 0;
        sortedLookup = dimValues;
        if (!allNulls) {
          sortedLookups[i] = dimValues.getSortedStrings();
          sortedLongLookups[i] = dimValues.getSortedLongs();
          sortedDoubleLookups[i] = dimValues.getSortedDoubles();
          sortedArrayLookups[i] = dimValues.getSortedArrays();
          numMergeIndex++;
        }
      }

      descriptorBuilder = new ColumnDescriptor.Builder();

      final NestedDataColumnSerializer defaultSerializer = new NestedDataColumnSerializer(
          name,
          indexSpec,
          segmentWriteOutMedium,
          progressIndicator,
          closer
      );
      serializer = defaultSerializer;

      final ComplexColumnPartSerde partSerde = ComplexColumnPartSerde.serializerBuilder()
                                                                     .withTypeName(NestedDataComplexTypeSerde.TYPE_NAME)
                                                                     .withDelegate(serializer)
                                                                     .build();
      descriptorBuilder.setValueType(ValueType.COMPLEX)
                       .setHasMultipleValues(false)
                       .addSerde(partSerde);

      defaultSerializer.open();
      defaultSerializer.serializeFields(mergedFields);

      int stringCardinality;
      int longCardinality;
      int doubleCardinality;
      int arrayCardinality;
      if (numMergeIndex == 1) {
        defaultSerializer.serializeStringDictionary(sortedLookup.getSortedStrings());
        defaultSerializer.serializeLongDictionary(sortedLookup.getSortedLongs());
        defaultSerializer.serializeDoubleDictionary(sortedLookup.getSortedDoubles());
        defaultSerializer.serializeArrayDictionary(() -> new ArrayDictionaryMergingIterator(
            sortedArrayLookups,
            defaultSerializer.getGlobalLookup()
        ));
        stringCardinality = sortedLookup.getStringCardinality();
        longCardinality = sortedLookup.getLongCardinality();
        doubleCardinality = sortedLookup.getDoubleCardinality();
        arrayCardinality = sortedLookup.getArrayCardinality();
      } else {
        SimpleDictionaryMergingIterator<String> dictionaryMergeIterator = new SimpleDictionaryMergingIterator<>(
            sortedLookups,
            STRING_MERGING_COMPARATOR
        );
        SimpleDictionaryMergingIterator<Long> longDictionaryMergeIterator = new SimpleDictionaryMergingIterator<>(
            sortedLongLookups,
            LONG_MERGING_COMPARATOR
        );
        SimpleDictionaryMergingIterator<Double> doubleDictionaryMergeIterator = new SimpleDictionaryMergingIterator<>(
            sortedDoubleLookups,
            DOUBLE_MERGING_COMPARATOR
        );
        defaultSerializer.serializeStringDictionary(() -> dictionaryMergeIterator);
        defaultSerializer.serializeLongDictionary(() -> longDictionaryMergeIterator);
        defaultSerializer.serializeDoubleDictionary(() -> doubleDictionaryMergeIterator);

        final ArrayDictionaryMergingIterator arrayDictionaryMergingIterator = new ArrayDictionaryMergingIterator(
            sortedArrayLookups,
            defaultSerializer.getGlobalLookup()
        );
        defaultSerializer.serializeArrayDictionary(() -> arrayDictionaryMergingIterator);
        stringCardinality = dictionaryMergeIterator.getCardinality();
        longCardinality = longDictionaryMergeIterator.getCardinality();
        doubleCardinality = doubleDictionaryMergeIterator.getCardinality();
        arrayCardinality = arrayDictionaryMergingIterator.getCardinality();
      }

      log.debug(
          "Completed dim[%s] conversions with string cardinality[%,d], long cardinality[%,d], double cardinality[%,d], array cardinality[%,d] in %,d millis.",
          name,
          stringCardinality,
          longCardinality,
          doubleCardinality,
          arrayCardinality,
          System.currentTimeMillis() - dimStartTime
      );
    }
    catch (Throwable ioe) {
      log.error(ioe, "Failed to merge dictionary for column [%s]", name);
      throw ioe;
    }
  }

  @Nullable
  private GlobalDictionarySortedCollector getSortedIndexFromIncrementalAdapter(
      IncrementalIndexAdapter adapter,
      SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> mergedFields
  )
  {
    final IncrementalIndex index = adapter.getIncrementalIndex();
    final IncrementalIndex.DimensionDesc dim = index.getDimension(name);
    if (dim == null || !(dim.getIndexer() instanceof NestedDataColumnIndexer)) {
      return null;
    }
    final NestedDataColumnIndexer indexer = (NestedDataColumnIndexer) dim.getIndexer();
    indexer.mergeFields(mergedFields);
    return indexer.getSortedCollector();
  }

  @Nullable
  private GlobalDictionarySortedCollector getSortedIndexesFromQueryableAdapter(
      QueryableIndexIndexableAdapter adapter,
      SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> mergedFields
  )
  {
    final ColumnHolder columnHolder = adapter.getQueryableIndex().getColumnHolder(name);

    if (columnHolder == null) {
      return null;
    }

    final BaseColumn col = columnHolder.getColumn();

    closer.register(col);

    if (col instanceof CompressedNestedDataComplexColumn) {
      return getSortedIndexFromV1QueryableAdapterNestedColumn(mergedFields, col);
    }
    return null;
  }

  private GlobalDictionarySortedCollector getSortedIndexFromV1QueryableAdapterNestedColumn(
      SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> mergedFields,
      BaseColumn col
  )
  {
    @SuppressWarnings("unchecked")
    CompressedNestedDataComplexColumn<?> column = (CompressedNestedDataComplexColumn) col;
    closer.register(column);
    for (int i = 0; i < column.getFields().size(); i++) {
      String fieldPath = column.getFields().get(i);
      NestedLiteralTypeInfo.TypeSet types = column.getFieldInfo().getTypes(i);
      mergedFields.compute(fieldPath, (k, v) -> {
        if (v == null) {
          return new NestedLiteralTypeInfo.MutableTypeSet(types.getByteValue());
        }
        return v.merge(types.getByteValue());
      });
    }
    return new GlobalDictionarySortedCollector(
        new StringEncodingStrategies.Utf8ToStringIndexed(column.getStringDictionary()),
        column.getLongDictionary(),
        column.getDoubleDictionary(),
        column.getArraysIterable(),
        column.getArrayDictionary().size()
    );
  }

  @Override
  public ColumnValueSelector convertSortedSegmentRowValuesToMergedRowValues(
      int segmentIndex,
      ColumnValueSelector source
  )
  {
    return source;
  }

  @Override
  public void processMergedRow(ColumnValueSelector selector) throws IOException
  {
    serializer.serialize(selector);
  }

  @Override
  public void writeIndexes(@Nullable List<IntBuffer> segmentRowNumConversions)
  {
    // fields write their own indexes
  }

  @Override
  public boolean hasOnlyNulls()
  {
    return false;
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor()
  {
    return descriptorBuilder.build();
  }

  private <T> boolean allNull(Indexed<T> dimValues)
  {
    for (int i = 0, size = dimValues.size(); i < size; i++) {
      if (dimValues.get(i) != null) {
        return false;
      }
    }
    return true;
  }

  public static class ArrayDictionaryMergingIterator implements Iterator<int[]>
  {
    private static final Comparator<PeekingIterator<int[]>> PEEKING_ITERATOR_COMPARATOR =
        (lhs, rhs) -> FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR.compare(lhs.peek(), rhs.peek());

    protected final PriorityQueue<PeekingIterator<int[]>> pQueue;
    protected int counter;

    public ArrayDictionaryMergingIterator(Iterable<Object[]>[] dimValueLookups, GlobalDictionaryIdLookup idLookup)
    {
      pQueue = new PriorityQueue<>(PEEKING_ITERATOR_COMPARATOR);

      for (Iterable<Object[]> dimValueLookup : dimValueLookups) {
        if (dimValueLookup == null) {
          continue;
        }
        final PeekingIterator<int[]> iter = Iterators.peekingIterator(
            new IdLookupArrayIterator(idLookup, dimValueLookup.iterator())
        );
        if (iter.hasNext()) {
          pQueue.add(iter);
        }
      }
    }

    @Override
    public boolean hasNext()
    {
      return !pQueue.isEmpty();
    }

    @Override
    public int[] next()
    {
      PeekingIterator<int[]> smallest = pQueue.remove();
      if (smallest == null) {
        throw new NoSuchElementException();
      }
      final int[] value = smallest.next();
      if (smallest.hasNext()) {
        pQueue.add(smallest);
      }

      while (!pQueue.isEmpty() && Arrays.equals(value, pQueue.peek().peek())) {
        PeekingIterator<int[]> same = pQueue.remove();
        same.next();
        if (same.hasNext()) {
          pQueue.add(same);
        }
      }
      counter++;

      return value;
    }

    public int getCardinality()
    {
      return counter;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove");
    }
  }

  public static class IdLookupArrayIterator implements Iterator<int[]>
  {
    private final GlobalDictionaryIdLookup idLookup;
    private final Iterator<Object[]> delegate;

    public IdLookupArrayIterator(
        GlobalDictionaryIdLookup idLookup,
        Iterator<Object[]> delegate
    )
    {
      this.idLookup = idLookup;
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext()
    {
      return delegate.hasNext();
    }

    @Override
    public int[] next()
    {
      final Object[] next = delegate.next();
      if (next == null) {
        return null;
      }
      final int[] newIdsWhoDis = new int[next.length];
      for (int i = 0; i < next.length; i++) {
        if (next[i] == null) {
          newIdsWhoDis[i] = 0;
        } else if (next[i] instanceof String) {
          newIdsWhoDis[i] = idLookup.lookupString((String) next[i]);
        } else if (next[i] instanceof Long) {
          newIdsWhoDis[i] = idLookup.lookupLong((Long) next[i]);
        } else if (next[i] instanceof Double) {
          newIdsWhoDis[i] = idLookup.lookupDouble((Double) next[i]);
        } else {
          newIdsWhoDis[i] = -1;
        }
        Preconditions.checkArgument(
            newIdsWhoDis[i] >= 0,
            "unknown global id [%s] for value [%s]",
            newIdsWhoDis[i],
            next[i]
        );
      }
      return newIdsWhoDis;
    }
  }
}
