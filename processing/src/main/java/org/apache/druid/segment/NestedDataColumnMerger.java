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

import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.nested.CompressedNestedDataComplexColumn;
import org.apache.druid.segment.nested.GlobalDictionarySortedCollector;
import org.apache.druid.segment.nested.NestedDataColumnSerializer;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedLiteralTypeInfo;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class NestedDataColumnMerger implements DimensionMergerV9
{
  private static final Logger log = new Logger(NestedDataColumnMerger.class);
  public static final Comparator<Pair<Integer, PeekingIterator<Long>>> LONG_MERGING_COMPARATOR =
      DictionaryMergingIterator.makePeekingComparator();
  public static final Comparator<Pair<Integer, PeekingIterator<Double>>> DOUBLE_MERGING_COMPARATOR =
      DictionaryMergingIterator.makePeekingComparator();

  private final String name;
  private final Closer closer;

  private NestedDataColumnSerializer serializer;

  public NestedDataColumnMerger(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ProgressIndicator progressIndicator,
      Closer closer
  )
  {

    this.name = name;
    this.serializer = new NestedDataColumnSerializer(name, indexSpec, segmentWriteOutMedium, progressIndicator, closer);
    this.closer = closer;
  }

  @Override
  public void writeMergedValueDictionary(List<IndexableAdapter> adapters) throws IOException
  {

    long dimStartTime = System.currentTimeMillis();

    int numMergeIndex = 0;
    GlobalDictionarySortedCollector sortedLookup = null;
    final Indexed[] sortedLookups = new Indexed[adapters.size()];
    final Indexed[] sortedLongLookups = new Indexed[adapters.size()];
    final Indexed[] sortedDoubleLookups = new Indexed[adapters.size()];

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

      boolean allNulls = allNull(dimValues.getSortedStrings()) &&
                         allNull(dimValues.getSortedLongs()) &&
                         allNull(dimValues.getSortedDoubles());
      sortedLookup = dimValues;
      if (!allNulls) {
        sortedLookups[i] = dimValues.getSortedStrings();
        sortedLongLookups[i] = dimValues.getSortedLongs();
        sortedDoubleLookups[i] = dimValues.getSortedDoubles();
        numMergeIndex++;
      }
    }

    serializer.open();
    serializer.serializeFields(mergedFields);

    int cardinality = 0;
    if (numMergeIndex > 1) {
      DictionaryMergingIterator<String> dictionaryMergeIterator = new DictionaryMergingIterator<>(
          sortedLookups,
          StringDimensionMergerV9.DICTIONARY_MERGING_COMPARATOR,
          true
      );
      DictionaryMergingIterator<Long> longDictionaryMergeIterator = new DictionaryMergingIterator<>(
          sortedLongLookups,
          LONG_MERGING_COMPARATOR,
          true
      );
      DictionaryMergingIterator<Double> doubleDictionaryMergeIterator = new DictionaryMergingIterator<>(
          sortedDoubleLookups,
          DOUBLE_MERGING_COMPARATOR,
          true
      );
      serializer.serializeStringDictionary(() -> dictionaryMergeIterator);
      serializer.serializeLongDictionary(() -> longDictionaryMergeIterator);
      serializer.serializeDoubleDictionary(() -> doubleDictionaryMergeIterator);
      cardinality = dictionaryMergeIterator.getCardinality();
    } else if (numMergeIndex == 1) {
      serializer.serializeStringDictionary(sortedLookup.getSortedStrings());
      serializer.serializeLongDictionary(sortedLookup.getSortedLongs());
      serializer.serializeDoubleDictionary(sortedLookup.getSortedDoubles());
      cardinality = sortedLookup.size();
    }

    log.debug(
        "Completed dim[%s] conversions with cardinality[%,d] in %,d millis.",
        name,
        cardinality,
        System.currentTimeMillis() - dimStartTime
    );
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
    for (Map.Entry<String, NestedDataColumnIndexer.LiteralFieldIndexer> entry : indexer.fieldIndexers.entrySet()) {
      // skip adding the field if no types are in the set, meaning only null values have been processed
      if (!entry.getValue().getTypes().isEmpty()) {
        mergedFields.put(entry.getKey(), entry.getValue().getTypes());
      }
    }
    return indexer.globalDictionary.getSortedCollector();
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
      return getSortedIndexFromV1QueryableAdapter(mergedFields, col);
    }
    return null;
  }

  private GlobalDictionarySortedCollector getSortedIndexFromV1QueryableAdapter(
      SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> mergedFields,
      BaseColumn col
  )
  {
    @SuppressWarnings("unchecked")
    CompressedNestedDataComplexColumn column = (CompressedNestedDataComplexColumn) col;
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
        column.getStringDictionary(),
        column.getLongDictionary(),
        column.getDoubleDictionary()
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
    return new ColumnDescriptor.Builder()
        .setValueType(ValueType.COMPLEX)
        .setHasMultipleValues(false)
        .addSerde(ComplexColumnPartSerde.serializerBuilder()
                                        .withTypeName(NestedDataComplexTypeSerde.TYPE_NAME)
                                        .withDelegate(serializer)
                                        .build()
        )
        .build();
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
}
