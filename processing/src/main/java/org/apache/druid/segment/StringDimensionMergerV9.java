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
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.collections.spatial.RTree;
import org.apache.druid.collections.spatial.split.LinearGutmanSplitStrategy;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.ColumnarIntsSerializer;
import org.apache.druid.segment.data.ColumnarMultiIntsSerializer;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ImmutableRTreeObjectStrategy;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSerializer;
import org.apache.druid.segment.data.VSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.VSizeColumnarMultiIntsSerializer;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringDimensionMergerV9 implements DimensionMergerV9
{
  private static final Logger log = new Logger(StringDimensionMergerV9.class);

  private static final Indexed<String> NULL_STR_DIM_VAL = new ListIndexed<>(Collections.singletonList(null));
  private static final Splitter SPLITTER = Splitter.on(",");

  private final String dimensionName;
  private final ProgressIndicator progress;
  private final Closer closer;
  private final IndexSpec indexSpec;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final MutableBitmap nullRowsBitmap;
  private final ColumnCapabilities capabilities;

  private int dictionarySize;
  private int rowCount = 0;
  private int cardinality = 0;
  private boolean hasNull = false;

  @Nullable
  private GenericIndexedWriter<ImmutableBitmap> bitmapWriter;
  @Nullable
  private ByteBufferWriter<ImmutableRTree> spatialWriter;
  @Nullable
  private ArrayList<IntBuffer> dimConversions;
  @Nullable
  private List<IndexableAdapter> adapters;
  @Nullable
  private IndexMerger.DictionaryMergeIterator dictionaryMergeIterator;
  @Nullable
  private ColumnarIntsSerializer encodedValueSerializer;
  @Nullable
  private GenericIndexedWriter<String> dictionaryWriter;
  @Nullable
  private String firstDictionaryValue;


  public StringDimensionMergerV9(
      String dimensionName,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress,
      Closer closer
  )
  {
    this.dimensionName = dimensionName;
    this.indexSpec = indexSpec;
    this.capabilities = capabilities;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();

    this.progress = progress;
    this.closer = closer;
  }

  @Override
  public void writeMergedValueDictionary(List<IndexableAdapter> adapters) throws IOException
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
      @SuppressWarnings("MustBeClosedChecker") // we register dimValues in the closer
          Indexed<String> dimValues = closer.register(adapters.get(i).getDimValueLookup(dimensionName));
      if (dimValues != null && !allNull(dimValues)) {
        dimHasValues = true;
        hasNull |= dimValues.indexOf(null) >= 0;
        dimValueLookups[i] = dimValueLookup = dimValues;
        numMergeIndex++;
      } else {
        dimAbsentFromSomeIndex = true;
      }
    }

    boolean convertMissingValues = dimHasValues && dimAbsentFromSomeIndex;

    /*
     * Ensure the empty str is always in the dictionary if the dimension was missing from one index but
     * has non-null values in another index.
     * This is done so that IndexMerger.toMergedIndexRowIterator() can convert null columns to empty strings
     * later on, to allow rows from indexes without a particular dimension to merge correctly with
     * rows from indexes with null/empty str values for that dimension.
     */
    if (convertMissingValues && !hasNull) {
      hasNull = true;
      dimValueLookups[adapters.size()] = dimValueLookup = NULL_STR_DIM_VAL;
      numMergeIndex++;
    }

    String dictFilename = StringUtils.format("%s.dim_values", dimensionName);
    dictionaryWriter = new GenericIndexedWriter<>(segmentWriteOutMedium, dictFilename, GenericIndexed.STRING_STRATEGY);
    firstDictionaryValue = null;
    dictionarySize = 0;
    dictionaryWriter.open();

    cardinality = 0;
    if (numMergeIndex > 1) {
      dictionaryMergeIterator = new IndexMerger.DictionaryMergeIterator(dimValueLookups, true);
      writeDictionary(() -> dictionaryMergeIterator);
      for (int i = 0; i < adapters.size(); i++) {
        if (dimValueLookups[i] != null && dictionaryMergeIterator.needConversion(i)) {
          dimConversions.set(i, dictionaryMergeIterator.conversions[i]);
        }
      }
      cardinality = dictionaryMergeIterator.counter;
    } else if (numMergeIndex == 1) {
      writeDictionary(dimValueLookup);
      cardinality = dimValueLookup.size();
    }

    log.debug(
        "Completed dim[%s] conversions with cardinality[%,d] in %,d millis.",
        dimensionName,
        cardinality,
        System.currentTimeMillis() - dimStartTime
    );

    setupEncodedValueWriter();
  }

  private void writeDictionary(Iterable<String> dictionaryValues) throws IOException
  {
    for (String value : dictionaryValues) {
      dictionaryWriter.write(value);
      value = NullHandling.emptyToNullIfNeeded(value);
      if (dictionarySize == 0) {
        firstDictionaryValue = value;
      }
      dictionarySize++;
    }
  }

  protected void setupEncodedValueWriter() throws IOException
  {
    final CompressionStrategy compressionStrategy = indexSpec.getDimensionCompression();

    String filenameBase = StringUtils.format("%s.forward_dim", dimensionName);
    if (capabilities.hasMultipleValues().isTrue()) {
      if (compressionStrategy != CompressionStrategy.UNCOMPRESSED) {
        encodedValueSerializer = V3CompressedVSizeColumnarMultiIntsSerializer.create(
            dimensionName,
            segmentWriteOutMedium,
            filenameBase,
            cardinality,
            compressionStrategy
        );
      } else {
        encodedValueSerializer =
            new VSizeColumnarMultiIntsSerializer(dimensionName, segmentWriteOutMedium, cardinality);
      }
    } else {
      if (compressionStrategy != CompressionStrategy.UNCOMPRESSED) {
        encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
            dimensionName,
            segmentWriteOutMedium,
            filenameBase,
            cardinality,
            compressionStrategy
        );
      } else {
        encodedValueSerializer = new VSizeColumnarIntsSerializer(segmentWriteOutMedium, cardinality);
      }
    }
    encodedValueSerializer.open();
  }

  @Override
  public ColumnValueSelector convertSortedSegmentRowValuesToMergedRowValues(
      int segmentIndex,
      ColumnValueSelector source
  )
  {
    IntBuffer converter = dimConversions.get(segmentIndex);
    if (converter == null) {
      return source;
    }
    DimensionSelector sourceDimensionSelector = (DimensionSelector) source;

    IndexedInts convertedRow = new IndexedInts()
    {
      @Override
      public int size()
      {
        return sourceDimensionSelector.getRow().size();
      }

      @Override
      public int get(int index)
      {
        return converter.get(sourceDimensionSelector.getRow().get(index));
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("source", source);
        inspector.visit("converter", converter);
      }
    };
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        return convertedRow;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("convertedRow", convertedRow);
      }

      @Override
      public ValueMatcher makeValueMatcher(String value)
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
        throw new UnsupportedOperationException();
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

      @Nullable
      @Override
      public Object getObject()
      {
        return sourceDimensionSelector.getObject();
      }

      @Override
      public Class classOfObject()
      {
        return sourceDimensionSelector.classOfObject();
      }
    };
  }

  @Override
  public void processMergedRow(ColumnValueSelector selector) throws IOException
  {
    IndexedInts row = getRow(selector);
    int rowSize = row.size();
    if (rowSize == 0) {
      nullRowsBitmap.add(rowCount);
    } else if (hasNull && isNullRow(row, rowSize)) {
      // If this dimension has the null/empty str in its dictionary, a row with nulls at all positions should also be
      // added to nullRowBitmap.
      nullRowsBitmap.add(rowCount);
    }
    if (encodedValueSerializer instanceof ColumnarMultiIntsSerializer) {
      ((ColumnarMultiIntsSerializer) encodedValueSerializer).addValues(row);
    } else {
      int value = row.size() == 0 ? 0 : row.get(0);
      ((SingleValueColumnarIntsSerializer) encodedValueSerializer).addValue(value);
    }
    rowCount++;
  }

  private static IndexedInts getRow(ColumnValueSelector s)
  {
    if (s instanceof DimensionSelector) {
      return ((DimensionSelector) s).getRow();
    } else if (s instanceof NilColumnValueSelector) {
      return IndexedInts.empty();
    } else {
      throw new ISE(
          "ColumnValueSelector[%s], only DimensionSelector or NilColumnValueSelector is supported",
          s.getClass()
      );
    }
  }

  private static boolean isNullRow(IndexedInts row, int size)
  {
    for (int i = 0; i < size; i++) {
      if (row.get(i) != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void writeIndexes(@Nullable List<IntBuffer> segmentRowNumConversions) throws IOException
  {
    if (!capabilities.hasBitmapIndexes()) {
      return;
    }

    long dimStartTime = System.currentTimeMillis();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

    String bmpFilename = StringUtils.format("%s.inverted", dimensionName);
    bitmapWriter = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        bmpFilename,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    bitmapWriter.open();
    bitmapWriter.setObjectsNotSorted();

    BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();

    RTree tree = null;
    boolean hasSpatial = capabilities.hasSpatialIndexes();
    if (hasSpatial) {
      spatialWriter = new ByteBufferWriter<>(
          segmentWriteOutMedium,
          new ImmutableRTreeObjectStrategy(bitmapFactory)
      );
      spatialWriter.open();
      tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bitmapFactory), bitmapFactory);
    }

    IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimensionName);

    //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
    for (int dictId = 0; dictId < dictionarySize; dictId++) {
      progress.progress();
      mergeBitmaps(
          segmentRowNumConversions,
          bitmapFactory,
          tree,
          hasSpatial,
          dictIdSeeker,
          dictId
      );
    }

    if (hasSpatial) {
      spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));
    }

    log.debug(
        "Completed dim[%s] inverted with cardinality[%,d] in %,d millis.",
        dimensionName,
        dictionarySize,
        System.currentTimeMillis() - dimStartTime
    );

    if (dictionaryMergeIterator != null) {
      dictionaryMergeIterator.close();
    }
  }

  void mergeBitmaps(
      @Nullable List<IntBuffer> segmentRowNumConversions,
      BitmapFactory bmpFactory,
      RTree tree,
      boolean hasSpatial,
      IndexSeeker[] dictIdSeeker,
      int dictId
  ) throws IOException
  {
    List<IntIterable> convertedInvertedIndexesToMerge = Lists.newArrayListWithCapacity(adapters.size());
    for (int j = 0; j < adapters.size(); ++j) {
      int seekedDictId = dictIdSeeker[j].seek(dictId);
      if (seekedDictId != IndexSeeker.NOT_EXIST) {
        IntIterable values;
        if (segmentRowNumConversions != null) {
          values = new ConvertingBitmapValues(
              adapters.get(j).getBitmapValues(dimensionName, seekedDictId),
              segmentRowNumConversions.get(j)
          );
        } else {
          BitmapValues bitmapValues = adapters.get(j).getBitmapValues(dimensionName, seekedDictId);
          values = bitmapValues::iterator;
        }
        convertedInvertedIndexesToMerge.add(values);
      }
    }

    MutableBitmap mergedIndexes = bmpFactory.makeEmptyMutableBitmap();
    List<IntIterator> convertedInvertedIndexesIterators = new ArrayList<>(convertedInvertedIndexesToMerge.size());
    for (IntIterable convertedInvertedIndexes : convertedInvertedIndexesToMerge) {
      convertedInvertedIndexesIterators.add(convertedInvertedIndexes.iterator());
    }

    // Merge ascending index iterators into a single one, remove duplicates, and add to the mergedIndexes bitmap.
    // Merge is needed, because some compacting MutableBitmap implementations are very inefficient when bits are
    // added not in the ascending order.
    int prevRow = IndexMerger.INVALID_ROW;
    for (IntIterator mergeIt = IntIteratorUtils.mergeAscending(convertedInvertedIndexesIterators);
         mergeIt.hasNext(); ) {
      int row = mergeIt.nextInt();
      if (row != prevRow && row != IndexMerger.INVALID_ROW) {
        mergedIndexes.add(row);
      }
      prevRow = row;
    }

    if (dictId == 0 && firstDictionaryValue == null) {
      mergedIndexes.or(nullRowsBitmap);
    }

    bitmapWriter.write(bmpFactory.makeImmutableBitmap(mergedIndexes));

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
  public boolean canSkip()
  {
    return cardinality == 0;
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

  protected interface IndexSeeker
  {
    int NOT_EXIST = -1;
    int NOT_INIT = -1;

    int seek(int dictId);
  }

  protected static class IndexSeekerWithoutConversion implements IndexSeeker
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
  protected static class IndexSeekerWithConversion implements IndexSeeker
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

    @Override
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

  public static class ConvertingBitmapValues implements IntIterable
  {
    private final BitmapValues baseValues;
    private final IntBuffer conversionBuffer;

    ConvertingBitmapValues(BitmapValues baseValues, IntBuffer conversionBuffer)
    {
      this.baseValues = baseValues;
      this.conversionBuffer = conversionBuffer;
    }

    @Nonnull
    @Override
    public IntIterator iterator()
    {
      final IntIterator baseIterator = baseValues.iterator();
      return new IntIterator()
      {
        @Override
        public boolean hasNext()
        {
          return baseIterator.hasNext();
        }

        @Override
        public int nextInt()
        {
          return conversionBuffer.get(baseIterator.nextInt());
        }

        @Override
        public int skip(int n)
        {
          return IntIteratorUtils.skip(baseIterator, n);
        }
      };
    }
  }

  protected IndexSeeker[] toIndexSeekers(
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
        try (CloseableIndexed<String> dimValueLookup = adapters.get(i).getDimValueLookup(dimension)) {
          seekers[i] = new IndexSeekerWithoutConversion(dimValueLookup == null ? 0 : dimValueLookup.size());
        }
        catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
    return seekers;
  }

  private boolean allNull(Indexed<String> dimValues)
  {
    for (int i = 0, size = dimValues.size(); i < size; i++) {
      if (dimValues.get(i) != null) {
        return false;
      }
    }
    return true;
  }
}
