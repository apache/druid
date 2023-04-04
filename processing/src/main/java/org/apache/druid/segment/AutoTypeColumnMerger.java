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
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexedWriter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.nested.DictionaryIdLookup;
import org.apache.druid.segment.nested.FieldTypeInfo;
import org.apache.druid.segment.nested.NestedCommonFormatColumn;
import org.apache.druid.segment.nested.NestedCommonFormatColumnSerializer;
import org.apache.druid.segment.nested.NestedDataColumnSerializer;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.ScalarDoubleColumnSerializer;
import org.apache.druid.segment.nested.ScalarLongColumnSerializer;
import org.apache.druid.segment.nested.ScalarStringColumnSerializer;
import org.apache.druid.segment.nested.SortedValueDictionary;
import org.apache.druid.segment.nested.VariantArrayColumnSerializer;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Column merger for {@link AutoTypeColumnIndexer} to eventually produce some form of
 * {@link NestedCommonFormatColumn}.
 * <p>
 * Depending on the types of values encountered
 */
public class AutoTypeColumnMerger implements DimensionMergerV9
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
  private NestedCommonFormatColumnSerializer serializer;

  private ColumnType logicalType;

  public AutoTypeColumnMerger(
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
      SortedValueDictionary sortedLookup = null;
      final Indexed[] sortedLookups = new Indexed[adapters.size()];
      final Indexed[] sortedLongLookups = new Indexed[adapters.size()];
      final Indexed[] sortedDoubleLookups = new Indexed[adapters.size()];
      final Indexed<Object[]>[] sortedArrayLookups = new Indexed[adapters.size()];

      final SortedMap<String, FieldTypeInfo.MutableTypeSet> mergedFields = new TreeMap<>();

      for (int i = 0; i < adapters.size(); i++) {
        final IndexableAdapter adapter = adapters.get(i);
        final IndexableAdapter.NestedColumnMergable mergable = closer.register(
            adapter.getNestedColumnMergeables(name)
        );
        final SortedValueDictionary dimValues = mergable.getValueDictionary();

        boolean allNulls = dimValues == null || dimValues.allNull();
        sortedLookup = dimValues;
        if (!allNulls) {
          mergable.mergeFieldsInto(mergedFields);
          sortedLookups[i] = dimValues.getSortedStrings();
          sortedLongLookups[i] = dimValues.getSortedLongs();
          sortedDoubleLookups[i] = dimValues.getSortedDoubles();
          sortedArrayLookups[i] = dimValues.getSortedArrays();
          numMergeIndex++;
        }
      }

      // check to see if we can specialize the serializer after merging all the adapters
      if (isSingleTypeRoot(mergedFields)) {
        logicalType = mergedFields.get(NestedPathFinder.JSON_PATH_ROOT).getSingleType();
        switch (logicalType.getType()) {
          case LONG:
            final ScalarLongColumnSerializer longSerializer = new ScalarLongColumnSerializer(
                name,
                indexSpec,
                segmentWriteOutMedium,
                progressIndicator,
                closer
            );
            serializer = longSerializer;
            break;
          case DOUBLE:
            final ScalarDoubleColumnSerializer doubleSerializer = new ScalarDoubleColumnSerializer(
                name,
                indexSpec,
                segmentWriteOutMedium,
                progressIndicator,
                closer
            );
            serializer = doubleSerializer;
            break;
          case STRING:
            final ScalarStringColumnSerializer stringSerializer = new ScalarStringColumnSerializer(
                name,
                indexSpec,
                segmentWriteOutMedium,
                progressIndicator,
                closer
            );
            serializer = stringSerializer;
            break;
          case ARRAY:
            final VariantArrayColumnSerializer arraySerializer = new VariantArrayColumnSerializer(
                name,
                indexSpec,
                segmentWriteOutMedium,
                progressIndicator,
                closer
            );
            serializer = arraySerializer;
            break;
          default:
            throw new ISE(
                "How did we get here? Column [%s] with type [%s] does not have specialized serializer",
                name,
                logicalType
            );
        }
      } else {
        // all the bells and whistles
        logicalType = ColumnType.NESTED_DATA;
        final NestedDataColumnSerializer defaultSerializer = new NestedDataColumnSerializer(
            name,
            indexSpec,
            segmentWriteOutMedium,
            progressIndicator,
            closer
        );
        serializer = defaultSerializer;
      }

      serializer.openDictionaryWriter();
      serializer.serializeFields(mergedFields);

      int stringCardinality;
      int longCardinality;
      int doubleCardinality;
      int arrayCardinality;
      if (numMergeIndex == 1) {
        serializer.serializeDictionaries(
            sortedLookup.getSortedStrings(),
            sortedLookup.getSortedLongs(),
            sortedLookup.getSortedDoubles(),
            () -> new ArrayDictionaryMergingIterator(
                sortedArrayLookups,
                serializer.getGlobalLookup()
            )
        );
        stringCardinality = sortedLookup.getStringCardinality();
        longCardinality = sortedLookup.getLongCardinality();
        doubleCardinality = sortedLookup.getDoubleCardinality();
        arrayCardinality = sortedLookup.getArrayCardinality();
      } else {
        final SimpleDictionaryMergingIterator<String> stringIterator = new SimpleDictionaryMergingIterator<>(
            sortedLookups,
            STRING_MERGING_COMPARATOR
        );
        final SimpleDictionaryMergingIterator<Long> longIterator = new SimpleDictionaryMergingIterator<>(
            sortedLongLookups,
            LONG_MERGING_COMPARATOR
        );
        final SimpleDictionaryMergingIterator<Double> doubleIterator = new SimpleDictionaryMergingIterator<>(
            sortedDoubleLookups,
            DOUBLE_MERGING_COMPARATOR
        );
        final ArrayDictionaryMergingIterator arrayIterator = new ArrayDictionaryMergingIterator(
            sortedArrayLookups,
            serializer.getGlobalLookup()
        );
        serializer.serializeDictionaries(
            () -> stringIterator,
            () -> longIterator,
            () -> doubleIterator,
            () -> arrayIterator
        );
        stringCardinality = stringIterator.getCardinality();
        longCardinality = longIterator.getCardinality();
        doubleCardinality = doubleIterator.getCardinality();
        arrayCardinality = arrayIterator.getCardinality();
      }
      // open main serializer after dictionaries have been serialized. we can't do this earlier since we don't know
      // dictionary cardinalities until after merging them, and we need to know that to configure compression and such
      // which depend on knowing the highest value
      serializer.open();

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
    catch (IOException ioe) {
      log.error(ioe, "Failed to merge dictionary for column [%s]", name);
      throw ioe;
    }
  }

  private static boolean isSingleTypeRoot(SortedMap<String, FieldTypeInfo.MutableTypeSet> mergedFields)
  {
    return mergedFields.size() == 1
           && mergedFields.get(NestedPathFinder.JSON_PATH_ROOT) != null
           && mergedFields.get(NestedPathFinder.JSON_PATH_ROOT).getSingleType() != null;
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
    ColumnDescriptor.Builder descriptorBuilder = new ColumnDescriptor.Builder();

    final NestedCommonFormatColumnPartSerde partSerde = NestedCommonFormatColumnPartSerde.serializerBuilder()
                                                                                         .withLogicalType(logicalType)
                                                                                         .withHasNulls(serializer.hasNulls())
                                                                                         .withByteOrder(ByteOrder.nativeOrder())
                                                                                         .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                                                                         .withSerializer(serializer)
                                                                                         .build();
    descriptorBuilder.setValueType(ValueType.COMPLEX) // this doesn't really matter... you could say.. its complicated..
                     .setHasMultipleValues(false)
                     .addSerde(partSerde);
    return descriptorBuilder.build();
  }

  public static class ArrayDictionaryMergingIterator implements Iterator<int[]>
  {
    private static final Comparator<PeekingIterator<int[]>> PEEKING_ITERATOR_COMPARATOR =
        (lhs, rhs) -> FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR.compare(lhs.peek(), rhs.peek());

    protected final PriorityQueue<PeekingIterator<int[]>> pQueue;
    private final Iterable<Object[]>[] dimValueLookups;
    private final DictionaryIdLookup idLookup;

    protected int counter;
    private boolean initialized;

    public ArrayDictionaryMergingIterator(Iterable<Object[]>[] dimValueLookups, DictionaryIdLookup idLookup)
    {
      this.pQueue = new PriorityQueue<>(PEEKING_ITERATOR_COMPARATOR);
      this.dimValueLookups = dimValueLookups;
      this.idLookup = idLookup;
    }

    private void initialize()
    {
      // we initialize lazily because the global id lookup might not be populated because the lower dictionary mergers
      // have not been iterated yet, so wait until we iterate this one while serializing to populate it
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
      initialized = true;
    }

    @Override
    public boolean hasNext()
    {
      if (!initialized) {
        initialize();
      }
      return !pQueue.isEmpty();
    }

    @Override
    public int[] next()
    {
      if (!initialized) {
        initialize();
      }
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
    private final DictionaryIdLookup idLookup;
    private final Iterator<Object[]> delegate;

    public IdLookupArrayIterator(
        DictionaryIdLookup idLookup,
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
      final int[] globalIds = new int[next.length];
      for (int i = 0; i < next.length; i++) {
        if (next[i] == null) {
          globalIds[i] = 0;
        } else if (next[i] instanceof String) {
          globalIds[i] = idLookup.lookupString((String) next[i]);
        } else if (next[i] instanceof Long) {
          globalIds[i] = idLookup.lookupLong((Long) next[i]);
        } else if (next[i] instanceof Double) {
          globalIds[i] = idLookup.lookupDouble((Double) next[i]);
        } else {
          globalIds[i] = -1;
        }
        Preconditions.checkArgument(
            globalIds[i] >= 0,
            "unknown global id [%s] for value [%s]",
            globalIds[i],
            next[i]
        );
      }
      return globalIds;
    }
  }
}
