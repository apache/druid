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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.collections.spatial.RTree;
import io.druid.collections.spatial.split.LinearGutmanSplitStrategy;
import io.druid.common.config.NullHandling;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.BitmapValues;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSerializer;
import io.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.ImmutableRTreeObjectStrategy;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.ColumnarIntsSerializer;
import io.druid.segment.data.VSizeColumnarIntsSerializer;
import io.druid.segment.data.VSizeColumnarMultiIntsSerializer;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public class StringDimensionMergerV9 implements DimensionMergerV9<int[]>
{
  private static final Logger log = new Logger(StringDimensionMergerV9.class);

  protected static final Indexed<String> NULL_STR_DIM_VAL = new ArrayIndexed<>(
      new String[]{(String) null},
      String.class
  );
  protected static final int[] NULL_STR_DIM_ARRAY = new int[]{0};
  private static final Splitter SPLITTER = Splitter.on(",");

  private ColumnarIntsSerializer encodedValueWriter;

  private String dimensionName;
  private GenericIndexedWriter<String> dictionaryWriter;
  private String firstDictionaryValue;
  private int dictionarySize;
  private GenericIndexedWriter<ImmutableBitmap> bitmapWriter;
  private ByteBufferWriter<ImmutableRTree> spatialWriter;
  private ArrayList<IntBuffer> dimConversions;
  private int cardinality = 0;
  private boolean convertMissingValues = false;
  private boolean hasNull = false;
  private MutableBitmap nullRowsBitmap;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private int rowCount = 0;
  private ColumnCapabilities capabilities;
  private List<IndexableAdapter> adapters;
  private ProgressIndicator progress;
  private final IndexSpec indexSpec;
  private IndexMerger.DictionaryMergeIterator dictionaryMergeIterator;

  public StringDimensionMergerV9(
      String dimensionName,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    this.dimensionName = dimensionName;
    this.indexSpec = indexSpec;
    this.capabilities = capabilities;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.progress = progress;
    nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
  }

  @Override
  public void writeMergedValueMetadata(List<IndexableAdapter> adapters) throws IOException
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
      Indexed<String> dimValues = (Indexed) adapters.get(i).getDimValueLookup(dimensionName);
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

    log.info(
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
    if (capabilities.hasMultipleValues()) {
      if (compressionStrategy != CompressionStrategy.UNCOMPRESSED) {
        encodedValueWriter = V3CompressedVSizeColumnarMultiIntsSerializer.create(
            segmentWriteOutMedium,
            filenameBase,
            cardinality,
            compressionStrategy
        );
      } else {
        encodedValueWriter = new VSizeColumnarMultiIntsSerializer(segmentWriteOutMedium, cardinality);
      }
    } else {
      if (compressionStrategy != CompressionStrategy.UNCOMPRESSED) {
        encodedValueWriter = CompressedVSizeColumnarIntsSerializer.create(
            segmentWriteOutMedium,
            filenameBase,
            cardinality,
            compressionStrategy
        );
      } else {
        encodedValueWriter = new VSizeColumnarIntsSerializer(segmentWriteOutMedium, cardinality);
      }
    }
    encodedValueWriter.open();
  }


  @Override
  public int[] convertSegmentRowValuesToMergedRowValues(int[] segmentRow, int segmentIndexNumber)
  {
    int[] dimVals = segmentRow;
    // For strings, convert missing values to null/empty if conversion flag is set
    // But if bitmap/dictionary is not used, always convert missing to 0
    if (dimVals == null) {
      return convertMissingValues ? NULL_STR_DIM_ARRAY : null;
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
  public void processMergedRow(int[] rowValues) throws IOException
  {
    int[] vals = rowValues;
    if (vals == null || vals.length == 0) {
      nullRowsBitmap.add(rowCount);
    } else if (hasNull && vals.length == 1 && (vals[0]) == 0) {
      // Dictionary encoded, so it's safe to cast dim value to integer
      // If this dimension has the null/empty str in its dictionary, a row with a single-valued dimension
      // that matches the null/empty str's dictionary ID should also be added to nullRowBitmap.
      nullRowsBitmap.add(rowCount);
    }
    processMergedRowHelper(vals);
    rowCount++;
  }

  protected void processMergedRowHelper(int[] vals) throws IOException
  {
    encodedValueWriter.add(vals);
  }

  @Override
  public void writeIndexes(List<IntBuffer> segmentRowNumConversions) throws IOException
  {
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

    log.info(
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
      List<IntBuffer> segmentRowNumConversions,
      BitmapFactory bmpFactory,
      RTree tree,
      boolean hasSpatial,
      IndexSeeker[] dictIdSeeker,
      int dictId
  ) throws IOException
  {
    List<ConvertingBitmapValues> convertedInvertedIndexesToMerge = Lists.newArrayListWithCapacity(adapters.size());
    for (int j = 0; j < adapters.size(); ++j) {
      int seekedDictId = dictIdSeeker[j].seek(dictId);
      if (seekedDictId != IndexSeeker.NOT_EXIST) {
        convertedInvertedIndexesToMerge.add(
            new ConvertingBitmapValues(
                adapters.get(j).getBitmapValues(dimensionName, seekedDictId),
                segmentRowNumConversions.get(j)
            )
        );
      }
    }

    MutableBitmap mergedIndexes = bmpFactory.makeEmptyMutableBitmap();
    List<IntIterator> convertedInvertedIndexesIterators = new ArrayList<>(convertedInvertedIndexesToMerge.size());
    for (ConvertingBitmapValues convertedInvertedIndexes : convertedInvertedIndexesToMerge) {
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
    boolean hasMultiValue = capabilities.hasMultipleValues();
    final CompressionStrategy compressionStrategy = indexSpec.getDimensionCompression();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

    final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
    builder.setValueType(ValueType.STRING);
    builder.setHasMultipleValues(hasMultiValue);
    final DictionaryEncodedColumnPartSerde.SerializerBuilder partBuilder = DictionaryEncodedColumnPartSerde
        .serializerBuilder()
        .withDictionary(dictionaryWriter)
        .withValue(
            encodedValueWriter,
            hasMultiValue,
            compressionStrategy != CompressionStrategy.UNCOMPRESSED
        )
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
        Indexed<String> dimValueLookup = (Indexed) adapters.get(i).getDimValueLookup(dimension);
        seekers[i] = new IndexSeekerWithoutConversion(dimValueLookup == null ? 0 : dimValueLookup.size());
      }
    }
    return seekers;
  }

  protected boolean isNullColumn(Iterable<String> dimValues)
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
}
