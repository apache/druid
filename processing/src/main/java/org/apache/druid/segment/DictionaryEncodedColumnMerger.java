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
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.ColumnarIntsSerializer;
import org.apache.druid.segment.data.ColumnarMultiIntsSerializer;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSerializer;
import org.apache.druid.segment.data.VSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.VSizeColumnarMultiIntsSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Base structure for merging dictionary encoded columns
 */
public abstract class DictionaryEncodedColumnMerger<T extends Comparable<T>> implements DimensionMergerV9
{
  private static final Logger log = new Logger(DictionaryEncodedColumnMerger.class);

  protected final String dimensionName;
  protected final ProgressIndicator progress;
  protected final Closer closer;
  protected final IndexSpec indexSpec;
  protected final SegmentWriteOutMedium segmentWriteOutMedium;
  protected final MutableBitmap nullRowsBitmap;
  protected final ColumnCapabilities capabilities;

  protected int dictionarySize;
  protected int rowCount = 0;
  protected int cardinality = 0;
  protected boolean hasNull = false;

  @Nullable
  protected GenericIndexedWriter<ImmutableBitmap> bitmapWriter;
  @Nullable
  protected ArrayList<IntBuffer> dimConversions;
  @Nullable
  protected List<IndexableAdapter> adapters;
  @Nullable
  protected DictionaryMergingIterator<T> dictionaryMergeIterator;
  @Nullable
  protected ColumnarIntsSerializer encodedValueSerializer;

  @Nullable
  protected DictionaryWriter<T> dictionaryWriter;

  @Nullable
  protected T firstDictionaryValue;


  public DictionaryEncodedColumnMerger(
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
    this.nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();

    this.progress = progress;
    this.closer = closer;
  }

  protected abstract Comparator<Pair<Integer, PeekingIterator<T>>> getDictionaryMergingComparator();
  protected abstract Indexed<T> getNullDimValue();
  protected abstract ObjectStrategy<T> getObjectStrategy();
  @Nullable
  protected abstract T coerceValue(T value);

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
    Indexed<T> dimValueLookup = null;
    Indexed<T>[] dimValueLookups = new Indexed[adapters.size() + 1];
    for (int i = 0; i < adapters.size(); i++) {
      @SuppressWarnings("MustBeClosedChecker") // we register dimValues in the closer
      Indexed<T> dimValues = closer.register(adapters.get(i).getDimValueLookup(dimensionName));
      if (dimValues != null && !allNull(dimValues)) {
        dimHasValues = true;
        hasNull = hasNull || dimValues.indexOf(null) >= 0;
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
      dimValueLookups[adapters.size()] = dimValueLookup = getNullDimValue();
      numMergeIndex++;
    }

    String dictFilename = StringUtils.format("%s.dim_values", dimensionName);
    dictionaryWriter = makeDictionaryWriter(dictFilename);
    firstDictionaryValue = null;
    dictionarySize = 0;
    dictionaryWriter.open();

    cardinality = 0;
    if (numMergeIndex > 1) {
      dictionaryMergeIterator = new DictionaryMergingIterator<>(
          dimValueLookups,
          getDictionaryMergingComparator(),
          true
      );
      writeDictionary(() -> dictionaryMergeIterator);
      for (int i = 0; i < adapters.size(); i++) {
        if (dimValueLookups[i] != null && dictionaryMergeIterator.needConversion(i)) {
          dimConversions.set(i, dictionaryMergeIterator.conversions[i]);
        }
      }
      cardinality = dictionaryMergeIterator.getCardinality();
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

    ExtendedIndexesMerger extendedIndexesMerger = getExtendedIndexesMerger();

    if (extendedIndexesMerger != null) {
      extendedIndexesMerger.initialize();
    }

    IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimensionName);

    //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
    for (int dictId = 0; dictId < dictionarySize; dictId++) {
      progress.progress();
      MutableBitmap mergedIndexes = mergeBitmaps(
          segmentRowNumConversions,
          bitmapFactory,
          dictIdSeeker,
          dictId
      );
      if (extendedIndexesMerger != null) {
        extendedIndexesMerger.mergeIndexes(dictId, mergedIndexes);
      }
    }

    if (extendedIndexesMerger != null) {
      extendedIndexesMerger.write();
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

  protected DictionaryWriter<T> makeDictionaryWriter(String fileName)
  {
    return new GenericIndexedWriter<>(segmentWriteOutMedium, fileName, getObjectStrategy());
  }

  @Nullable
  protected ExtendedIndexesMerger getExtendedIndexesMerger()
  {
    return null;
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

  protected void writeDictionary(Iterable<T> dictionaryValues) throws IOException
  {
    for (T value : dictionaryValues) {
      dictionaryWriter.write(value);
      value = coerceValue(value);
      if (dictionarySize == 0) {
        firstDictionaryValue = value;
      }
      dictionarySize++;
    }
  }

  protected MutableBitmap mergeBitmaps(
      @Nullable List<IntBuffer> segmentRowNumConversions,
      BitmapFactory bmpFactory,
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

    return mergedIndexes;
  }

  @Override
  public boolean hasOnlyNulls()
  {
    return cardinality == 0;
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

  private boolean allNull(Indexed<T> dimValues)
  {
    for (int i = 0, size = dimValues.size(); i < size; i++) {
      if (dimValues.get(i) != null) {
        return false;
      }
    }
    return true;
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

  /**
   * Specifies any additional per value indexes which should be constructed when
   * {@link DictionaryEncodedColumnMerger#writeIndexes(List)} is called, on top of the standard bitmap index created
   * with {@link DictionaryEncodedColumnMerger#mergeBitmaps}
   */
  interface ExtendedIndexesMerger
  {
    void initialize() throws IOException;

    /**
     * Merge extended indexes for the given dictionaryId value. The merged bitmap index from
     * {@link DictionaryEncodedColumnMerger#mergeBitmaps} is supplied should it be useful for the construction of
     * the extended indexes.
     */
    void mergeIndexes(int dictId, MutableBitmap mergedIndexes) throws IOException;
    void write() throws IOException;
  }
}
