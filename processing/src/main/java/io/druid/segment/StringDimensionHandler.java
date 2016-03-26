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
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.io.smoosh.FileSmoosher;
import com.metamx.common.io.smoosh.SmooshedWriter;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.collections.CombiningIterable;
import io.druid.common.guava.FileOutputSupplier;
import io.druid.common.utils.SerializerUtils;
import io.druid.query.dimension.DimensionSpec;
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
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.commons.lang.ObjectUtils;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringDimensionHandler implements DimensionHandler<Integer, String>
{
  private static final Logger log = new Logger(StringDimensionHandler.class);

  private static final ListIndexed EMPTY_STR_DIM_VAL = new ListIndexed<>(Arrays.asList(""), String.class);
  protected static final Splitter SPLITTER = Splitter.on(",");

  private final String dimensionName;

  public StringDimensionHandler(String dimensionName)
  {
    this.dimensionName = dimensionName;
  }

  @Override
  public DimensionSelector getDimensionSelector(ColumnSelectorFactory cursor, DimensionSpec dimSpec, ColumnCapabilities capabilities)
  {
    return cursor.makeDictEncodedStringDimensionSelector(dimSpec);
  }

  @Override
  public Function<Object, String> getValueTypeTransformer()
  {
    return STRING_TRANSFORMER;
  }

  @Override
  public Comparator<Integer> getEncodedComparator()
  {
    return ENCODED_COMPARATOR;
  }

  @Override
  public Comparator<String> getActualComparator()
  {
    return UNENCODED_COMPARATOR;
  }

  @Override
  public byte[] getBytesFromRowValue(Integer rowVal)
  {
    return Ints.toByteArray(rowVal);
  }

  @Override
  public Integer getRowValueFromBytes(byte[] bytes)
  {
    return Ints.fromByteArray(bytes);
  }

  @Override
  public int getEncodedValueSize()
  {
    return Ints.BYTES;
  }

  @Override
  public DimensionIndexer makeIndexer(Object lock)
  {
    return new StringDimensionIndexer(lock);
  }

  @Override
  public DimensionMerger makeMerger(
      IndexSpec indexSpec,
      File outDir,
      IOPeon ioPeon,
      FileSmoosher v9Smoosher,
      ObjectMapper mapper,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    return new StringDimensionMerger(indexSpec, outDir, ioPeon, v9Smoosher, mapper, capabilities, progress);
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
      return Integer.compare(o1, o2);
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
      return ObjectUtils.compare(o1, o2);
    }
  };

  private class DimensionDictionary
  {
    private String minValue = null;
    private String maxValue = null;

    private final Map<String, Integer> valueToId = Maps.newHashMap();

    private final List<String> idToValue = Lists.newArrayList();
    private final Object lock;

    public DimensionDictionary(Object lock)
    {
      this.lock = lock;
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

  private class SortedDimensionDictionary
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
    @Override
    public void addNullLookup()
    {
      dimLookup.add(null);
      sortedLookup = dimLookup.sort();
    }

    private DimensionDictionary dimLookup;
    private SortedDimensionDictionary sortedLookup;
    private Object lock;

    public StringDimensionIndexer(Object lock)
    {
      this.lock = lock;
      this.dimLookup = new DimensionDictionary(this.lock);
    }

    @Override
    public Comparable[] processRowValsToIndexKey(Object dimValues)
    {
      final Comparable[] dimensionValues;
      String transformedVal;
      int oldDictSize = dimLookup.size();
      try {
        if (dimValues == null) {
          dimLookup.add(null);
          dimensionValues = null;
        } else if (dimValues instanceof List) {
          List<Object> dimValuesList = (List) dimValues;
          dimensionValues = new Comparable[dimValuesList.size()];
          for (int i = 0; i < dimValuesList.size(); i++) {
            transformedVal = STRING_TRANSFORMER.apply(dimValuesList.get(i));
            dimensionValues[i] = dimLookup.add(transformedVal);
          }
        } else {
          transformedVal = STRING_TRANSFORMER.apply(dimValues);
          dimensionValues = new Comparable[]{dimLookup.add(transformedVal)};
        }
      }
      catch (ParseException pe) {
        throw new ParseException(pe.getMessage() + dimensionName);
      }

      // If dictionary size has changed, the sorted lookup is no longer valid.
      if (oldDictSize != dimLookup.size()) {
        sortedLookup = null;
      }

      return dimensionValues;
    }

    @Override
    public String getActualValue(Integer intermediateValue, boolean idSorted)
    {
      if (idSorted) {
        if (sortedLookup == null) {
          sortedLookup = dimLookup.sort();
        }
        return sortedLookup.getValueFromSortedId(intermediateValue);
      } else {
        return dimLookup.getValue(intermediateValue);

      }
    }

    @Override
    public Integer getEncodedValue(String fullValue, boolean idSorted)
    {
      int unsortedId = dimLookup.getId(fullValue);

      if (idSorted) {
        if (sortedLookup == null) {
          sortedLookup = dimLookup.sort();
        }
        return sortedLookup.getSortedIdFromUnsortedId(unsortedId);
      } else {
        return unsortedId;
      }
    }

    @Override
    public Integer getSortedEncodedValueFromUnsorted(Integer unsortedIntermediateValue)
    {
      if (sortedLookup == null) {
        sortedLookup = dimLookup.sort();
      }
      return sortedLookup.getSortedIdFromUnsortedId(unsortedIntermediateValue);
    }

    @Override
    public Integer getUnsortedEncodedValueFromSorted(Integer sortedIntermediateValue)
    {
      if (sortedLookup == null) {
        sortedLookup = dimLookup.sort();
      }
      return sortedLookup.getUnsortedIdFromSortedId(sortedIntermediateValue);
    }

    @Override
    public Indexed<String> getSortedIndexedValues()
    {
      if(sortedLookup == null) {
        sortedLookup = dimLookup.sort();
      }

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
          Comparable val = getActualValue(index, true);
          String strVal = val != null ? val.toString() : null;
          return strVal;
        }

        @Override
        public int indexOf(String value)
        {
          int id = (Integer) getEncodedValue(value, false);
          return id < 0 ? -1 : (Integer) getSortedEncodedValueFromUnsorted(id);
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
  }


  public class StringDimensionMerger implements DimensionMerger<Integer, String>
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
    private final FileSmoosher v9Smoosher;
    private List<IndexableAdapter> adapters;
    private ObjectMapper mapper;
    private ProgressIndicator progress;

    // Remove these once V8 IndexMerger is removed
    private boolean useV8 = false;
    private VSizeIndexedWriter encodedValueWriterV8;

    private final IndexSpec indexSpec;

    public StringDimensionMerger(
        IndexSpec indexSpec,
        File outDir,
        IOPeon ioPeon,
        FileSmoosher v9Smoosher,
        ObjectMapper mapper,
        ColumnCapabilities capabilities,
        ProgressIndicator progress
    )
    {
      this.indexSpec = indexSpec;
      this.capabilities = capabilities;
      this.outDir = outDir;
      this.ioPeon = ioPeon;
      this.v9Smoosher = v9Smoosher;
      this.mapper = mapper;
      this.progress = progress;
      nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }

    @Override
    public void initWriters() throws IOException
    {
      String dictFilename = String.format("%s.dim_values", dimensionName);
      dictionaryWriter = new GenericIndexedWriter<>(
          ioPeon,
          dictFilename,
          GenericIndexed.STRING_STRATEGY
      );
      dictionaryWriter.open();

      String bmpFilename = String.format("%s.inverted", dimensionName);
      bitmapWriter = new GenericIndexedWriter<>(
          ioPeon,
          bmpFilename,
          indexSpec.getBitmapSerdeFactory().getObjectStrategy()
      );
      bitmapWriter.open();
    }

    @Override
    public void mergeAcrossSegments(List<IndexableAdapter> adapters) throws IOException
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

      if (useV8) {
        encodedValueWriterV8 = new VSizeIndexedWriter(ioPeon, dimensionName, cardinality);
        encodedValueWriterV8.open();
        return;
      }

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
    public void addColumnValue(Comparable[] vals) throws IOException
    {
      if (vals == null || vals.length == 0) {
        nullRowsBitmap.add(rowCount);
      } else if (hasNull && vals.length == 1 && ((Integer) vals[0]) == 0) {
        // Dictionary encoded, so it's safe to cast dim value to integer
        // If this dimension has the null/empty str in its dictionary, a row with a single-valued dimension
        // that matches the null/empty str's dictionary ID should also be added to nullRowBitmap.
        nullRowsBitmap.add(rowCount);
      }
      if (useV8) {
        List<Comparable> listToWrite = (vals == null)
                                       ? null
                                       : Lists.newArrayList(vals);
        encodedValueWriterV8.add(listToWrite);
      } else {
        encodedValueWriter.add(vals);
      }

      rowCount++;
    }

    @Override
    public void closeWritersV8(List<IntBuffer> rowNumConversions, OutputSupplier<FileOutputStream> invertedOut, OutputSupplier<FileOutputStream> spatialOut) throws IOException
    {
      final SerializerUtils serializerUtils = new SerializerUtils();
      long dimStartTime = System.currentTimeMillis();

      dictionaryWriter.close();

      FileOutputSupplier dimOut = new FileOutputSupplier(IndexIO.makeDimFile(outDir, dimensionName), true);
      serializerUtils.writeString(dimOut, dimensionName);
      ByteStreams.copy(dictionaryWriter.combineStreams(), dimOut);
      File dimOutFile = dimOut.getFile();

      final MappedByteBuffer dimValsMapped = Files.map(dimOutFile);

      if (!dimensionName.equals(serializerUtils.readString(dimValsMapped))) {
        throw new ISE("dimensions[%s] didn't equate!?  This is a major WTF moment.", dimensionName);
      }
      Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, GenericIndexed.STRING_STRATEGY);
      log.info("Starting dimension[%s] with cardinality[%,d]", dimensionName, dimVals.size());

      final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
      final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();

      RTree tree = null;
      ByteBufferWriter<ImmutableRTree> spatialWriter = null;
      boolean hasSpatial = capabilities.hasSpatialIndexes();
      IOPeon spatialIoPeon = new TmpFileIOPeon();
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
                    adapters.get(j).getBitmapIndex(dimensionName, seekedDictId), rowNumConversions.get(j)
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
      bitmapWriter.close();
      serializerUtils.writeString(invertedOut, dimensionName);
      ByteStreams.copy(bitmapWriter.combineStreams(), invertedOut);


      log.info("Completed dimension[%s] in %,d millis.", dimensionName, System.currentTimeMillis() - dimStartTime);

      if (hasSpatial) {
        spatialWriter.write(ImmutableRTree.newImmutableFromMutable(tree));
        spatialWriter.close();

        serializerUtils.writeString(spatialOut, dimensionName);
        ByteStreams.copy(spatialWriter.combineStreams(), spatialOut);
        spatialIoPeon.cleanup();
      }


      encodedValueWriterV8.close();
      ByteStreams.copy(encodedValueWriterV8.combineStreams(), dimOut);


      return;
    }

    @Override
    public void closeWriters(List<IntBuffer> rowNumConversions) throws IOException
    {
      long dimStartTime = System.currentTimeMillis();
      final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

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
                    adapters.get(j).getBitmapIndex(dimensionName, seekedDictId), rowNumConversions.get(j)
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

      // Now write everything
      boolean hasMultiValue = capabilities.hasMultipleValues();
      final CompressedObjectStrategy.CompressionStrategy compressionStrategy = indexSpec.getDimensionCompressionStrategy();

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
      makeColumn(v9Smoosher, dimensionName, serdeficator);
      log.info("Completed dimension column[%s] in %,d millis.", dimensionName, System.currentTimeMillis() - dimStartTime);
    }

    @Override
    public boolean needsConvertMissingValues()
    {
      return convertMissingValues;
    }

    @Override
    public boolean canSkip()
    {
      return cardinality == 0;
    }

    @Override
    public Integer getConvertedEncodedValue(Integer value, int indexNumber)
    {
      IntBuffer converter = dimConversions.get(indexNumber);
      if (converter != null) {
        Integer convertedValue = converter.get(value);
        return convertedValue;
      }
      return converter == null ? value : converter.get(value);
    }

    @Override
    public void setWriteV8()
    {
      useV8 = true;
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

    private void makeColumn(
        final FileSmoosher v9Smoosher,
        final String columnName,
        final ColumnDescriptor serdeficator
    ) throws IOException
    {
      final SerializerUtils serializerUtils = new SerializerUtils();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
      byte[] specBytes = baos.toByteArray();

      final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
          columnName, serdeficator.numBytes() + specBytes.length
      );
      try {
        channel.write(ByteBuffer.wrap(specBytes));
        serdeficator.write(channel);
      }
      finally {
        channel.close();
      }
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
    private final DictionaryEncodedColumn dictionary;
    public StringDimensionColumnReader(Column column)
    {
      this.column = column;
      this.dictionary = column.getDictionaryEncoding();
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String value, BitmapFactory bitmapFactory, int numRows)
    {
      if (column == null) {
        if (Strings.isNullOrEmpty(value)) {
          return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), numRows);
        } else {
          return bitmapFactory.makeEmptyImmutableBitmap();
        }
      }

      return column.getBitmapIndex().getBitmap(value);
    }

    @Override
    public ImmutableBitmap getBitmapIndex(Predicate predicate, BitmapFactory bitmapFactory)
    {
      Indexed<String> dimValues = getSortedIndexedValues();
      if (dimValues == null || dimValues.size() == 0) {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }

      return bitmapFactory.union(
          FunctionalIterable.create(dimValues)
                            .filter(predicate)
                            .transform(
                                new Function<String, ImmutableBitmap>()
                                {
                                  @Override
                                  public ImmutableBitmap apply(@Nullable String input)
                                  {
                                    return column.getBitmapIndex().getBitmap(input);
                                  }
                                }
                            )
      );
    }

    @Override
    public ImmutableBitmap getBitmapIndex(Bound bound, BitmapFactory bitmapFactory)
    {
      if (!getCapabilities().hasSpatialIndexes()) {
        throw new UnsupportedOperationException("Column does not support spatial filtering: " + dimensionName);
      }
      ImmutableRTree spatialIndex = column.getSpatialIndex().getRTree();
      Iterable<ImmutableBitmap> search = spatialIndex.search(bound);
      return bitmapFactory.union(search);
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
    public Indexed<String> getSortedIndexedValues()
    {
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
