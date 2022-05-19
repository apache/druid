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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.collections.spatial.RTree;
import org.apache.druid.collections.spatial.split.LinearGutmanSplitStrategy;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ImmutableRTreeObjectStrategy;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class StringDimensionMergerV9 extends DictionaryEncodedColumnMerger<String>
{
  private static final Indexed<String> NULL_STR_DIM_VAL = new ListIndexed<>(Collections.singletonList(null));
  private static final Splitter SPLITTER = Splitter.on(",");

  public static final Comparator<Pair<Integer, PeekingIterator<String>>> DICTIONARY_MERGING_COMPARATOR =
      DictionaryMergingIterator.makePeekingComparator();

  @Nullable
  private ByteBufferWriter<ImmutableRTree> spatialWriter;

  public StringDimensionMergerV9(
      String dimensionName,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress,
      Closer closer
  )
  {
    super(dimensionName, indexSpec, segmentWriteOutMedium, capabilities, progress, closer);
  }

  @Override
  protected Comparator<Pair<Integer, PeekingIterator<String>>> getDictionaryMergingComparator()
  {
    return DICTIONARY_MERGING_COMPARATOR;
  }

  @Override
  protected Indexed<String> getNullDimValue()
  {
    return NULL_STR_DIM_VAL;
  }

  @Override
  protected ObjectStrategy<String> getObjectStrategy()
  {
    return GenericIndexed.STRING_STRATEGY;
  }

  @Override
  protected String coerceValue(String value)
  {
    return NullHandling.emptyToNullIfNeeded(value);
  }

  @Nullable
  @Override
  protected ExtendedIndexesMerger getExtendedIndexesMerger()
  {
    return new SpatialIndexesMerger();
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor()
  {
    // Now write everything
    boolean hasMultiValue = capabilities.hasMultipleValues().isTrue();
    final CompressionStrategy compressionStrategy = indexSpec.getDimensionCompression();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

    final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
    builder.setValueType(ValueType.STRING);
    builder.setHasMultipleValues(hasMultiValue);
    final DictionaryEncodedColumnPartSerde.SerializerBuilder partBuilder = DictionaryEncodedColumnPartSerde
        .serializerBuilder()
        .withDictionary(dictionaryWriter)
        .withValue(
            encodedValueSerializer,
            hasMultiValue,
            compressionStrategy != CompressionStrategy.UNCOMPRESSED
        )
        .withBitmapSerdeFactory(bitmapSerdeFactory)
        .withBitmapIndex(bitmapWriter)
        .withSpatialIndex(spatialWriter)
        .withByteOrder(IndexIO.BYTE_ORDER);

    return builder
        .addSerde(partBuilder.build())
        .build();
  }

  /**
   * Write spatial indexes for string columns that have them
   */
  public class SpatialIndexesMerger implements ExtendedIndexesMerger
  {
    private RTree tree;
    private final boolean hasSpatial = capabilities.hasSpatialIndexes();

    @Override
    public void initialize() throws IOException
    {
      BitmapFactory bitmapFactory = indexSpec.getBitmapSerdeFactory().getBitmapFactory();
      if (hasSpatial) {
        spatialWriter = new ByteBufferWriter<>(
            segmentWriteOutMedium,
            new ImmutableRTreeObjectStrategy(bitmapFactory)
        );
        spatialWriter.open();
        tree = new RTree(
            2,
            new LinearGutmanSplitStrategy(0, 50, bitmapFactory),
            bitmapFactory
        );
      }
    }

    @Override
    public void mergeIndexes(int dictId, MutableBitmap mergedIndexes) throws IOException
    {
      if (hasSpatial) {
        String dimVal = dictionaryWriter.get(dictId);
        if (dimVal != null) {
          List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
          float[] coords = new float[stringCoords.size()];
          for (int j = 0; j < coords.length; j++) {
            coords[j] = Float.valueOf(stringCoords.get(j));
          }
          tree.insert(coords, mergedIndexes);
        }
      }
    }

    @Override
    public void write() throws IOException
    {
      if (hasSpatial) {
        spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));
      }
    }
  }
}
