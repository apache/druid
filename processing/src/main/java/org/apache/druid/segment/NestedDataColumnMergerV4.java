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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.nested.FieldTypeInfo;
import org.apache.druid.segment.nested.NestedDataColumnSerializerV4;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.SortedValueDictionary;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class NestedDataColumnMergerV4 implements DimensionMergerV9
{
  private static final Logger log = new Logger(NestedDataColumnMergerV4.class);

  public static final Comparator<PeekingIterator<String>> STRING_MERGING_COMPARATOR =
      SimpleDictionaryMergingIterator.makePeekingComparator();
  public static final Comparator<PeekingIterator<Long>> LONG_MERGING_COMPARATOR =
      SimpleDictionaryMergingIterator.makePeekingComparator();
  public static final Comparator<PeekingIterator<Double>> DOUBLE_MERGING_COMPARATOR =
      SimpleDictionaryMergingIterator.makePeekingComparator();

  private final String name;
  private final IndexSpec indexSpec;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final Closer closer;

  private ColumnDescriptor.Builder descriptorBuilder;
  private NestedDataColumnSerializerV4 serializer;

  public NestedDataColumnMergerV4(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      Closer closer
  )
  {

    this.name = name;
    this.indexSpec = indexSpec;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
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

      final SortedMap<String, FieldTypeInfo.MutableTypeSet> mergedFields = new TreeMap<>();

      for (int i = 0; i < adapters.size(); i++) {
        final IndexableAdapter adapter = adapters.get(i);

        final IndexableAdapter.NestedColumnMergable mergable = closer.register(
            adapter.getNestedColumnMergeables(name)
        );
        if (mergable == null) {
          continue;
        }
        final SortedValueDictionary dimValues = mergable.getValueDictionary();

        boolean allNulls = dimValues == null || dimValues.allNull();
        if (!allNulls) {
          sortedLookup = dimValues;
          mergable.mergeFieldsInto(mergedFields);
          sortedLookups[i] = dimValues.getSortedStrings();
          sortedLongLookups[i] = dimValues.getSortedLongs();
          sortedDoubleLookups[i] = dimValues.getSortedDoubles();
          numMergeIndex++;
        }
      }

      descriptorBuilder = new ColumnDescriptor.Builder();

      serializer = new NestedDataColumnSerializerV4(
          name,
          indexSpec,
          segmentWriteOutMedium,
          closer
      );

      final ComplexColumnPartSerde partSerde = ComplexColumnPartSerde.serializerBuilder()
                                                                     .withTypeName(NestedDataComplexTypeSerde.TYPE_NAME)
                                                                     .withDelegate(serializer)
                                                                     .build();
      descriptorBuilder.setValueType(ValueType.COMPLEX)
                       .setHasMultipleValues(false)
                       .addSerde(partSerde);

      serializer.open();
      serializer.serializeFields(mergedFields);

      int stringCardinality;
      int longCardinality;
      int doubleCardinality;
      if (numMergeIndex == 1) {
        serializer.serializeDictionaries(
            sortedLookup.getSortedStrings(),
            sortedLookup.getSortedLongs(),
            sortedLookup.getSortedDoubles()
        );
        stringCardinality = sortedLookup.getStringCardinality();
        longCardinality = sortedLookup.getLongCardinality();
        doubleCardinality = sortedLookup.getDoubleCardinality();
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
        serializer.serializeDictionaries(
            () -> stringIterator,
            () -> longIterator,
            () -> doubleIterator
        );
        stringCardinality = stringIterator.getCardinality();
        longCardinality = longIterator.getCardinality();
        doubleCardinality = doubleIterator.getCardinality();
      }

      log.debug(
          "Completed dim[%s] conversions with string cardinality[%,d], long cardinality[%,d], double cardinality[%,d] in %,d millis.",
          name,
          stringCardinality,
          longCardinality,
          doubleCardinality,
          System.currentTimeMillis() - dimStartTime
      );
    }
    catch (IOException ioe) {
      log.error(ioe, "Failed to merge dictionary for column [%s]", name);
      throw ioe;
    }
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
}
