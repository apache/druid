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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.Smoosh;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;
import io.druid.segment.serde.BitmapIndexColumnPartSupplier;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.DictionaryEncodedColumnSupplier;
import io.druid.segment.serde.FloatGenericColumnPartSerde;
import io.druid.segment.serde.FloatGenericColumnSupplier;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import io.druid.segment.serde.LongGenericColumnSupplier;
import io.druid.segment.serde.SpatialIndexColumnPartSupplier;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexIO
{
  public static final byte V8_VERSION = 0x8;
  public static final byte V9_VERSION = 0x9;
  public static final int CURRENT_VERSION_ID = V9_VERSION;

  public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

  private final Map<Integer, IndexLoader> indexLoaders;

  private static final EmittingLogger log = new EmittingLogger(IndexIO.class);
  private static final SerializerUtils serializerUtils = new SerializerUtils();

  private final ObjectMapper mapper;
  private final DefaultIndexIOHandler defaultIndexIOHandler;
  private final ColumnConfig columnConfig;

  @Inject
  public IndexIO(ObjectMapper mapper, ColumnConfig columnConfig)
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    this.columnConfig = Preconditions.checkNotNull(columnConfig, "null ColumnConfig");
    defaultIndexIOHandler = new DefaultIndexIOHandler(mapper);
    indexLoaders = ImmutableMap.<Integer, IndexLoader>builder()
        .put(0, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(1, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(2, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(3, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(4, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(5, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(6, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(7, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(8, new LegacyIndexLoader(defaultIndexIOHandler, columnConfig))
        .put(9, new V9IndexLoader(columnConfig))
        .build();


  }

  public void validateTwoSegments(File dir1, File dir2) throws IOException
  {
    try (QueryableIndex queryableIndex1 = loadIndex(dir1)) {
      try (QueryableIndex queryableIndex2 = loadIndex(dir2)) {
        validateTwoSegments(
            new QueryableIndexIndexableAdapter(queryableIndex1),
            new QueryableIndexIndexableAdapter(queryableIndex2)
        );
      }
    }
  }

  public void validateTwoSegments(final IndexableAdapter adapter1, final IndexableAdapter adapter2)
  {
    if (adapter1.getNumRows() != adapter2.getNumRows()) {
      throw new SegmentValidationException(
          "Row count mismatch. Expected [%d] found [%d]",
          adapter1.getNumRows(),
          adapter2.getNumRows()
      );
    }
    {
      final Set<String> dimNames1 = Sets.newHashSet(adapter1.getDimensionNames());
      final Set<String> dimNames2 = Sets.newHashSet(adapter2.getDimensionNames());
      if (!dimNames1.equals(dimNames2)) {
        throw new SegmentValidationException(
            "Dimension names differ. Expected [%s] found [%s]",
            dimNames1,
            dimNames2
        );
      }
      final Set<String> metNames1 = Sets.newHashSet(adapter1.getMetricNames());
      final Set<String> metNames2 = Sets.newHashSet(adapter2.getMetricNames());
      if (!metNames1.equals(metNames2)) {
        throw new SegmentValidationException("Metric names differ. Expected [%s] found [%s]", metNames1, metNames2);
      }
    }
    final Map<String, DimensionHandler> dimHandlers = adapter1.getDimensionHandlers();

    final Iterator<Rowboat> it1 = adapter1.getRows().iterator();
    final Iterator<Rowboat> it2 = adapter2.getRows().iterator();
    long row = 0L;
    while (it1.hasNext()) {
      if (!it2.hasNext()) {
        throw new SegmentValidationException("Unexpected end of second adapter");
      }
      final Rowboat rb1 = it1.next();
      final Rowboat rb2 = it2.next();
      ++row;
      if (rb1.getRowNum() != rb2.getRowNum()) {
        throw new SegmentValidationException("Row number mismatch: [%d] vs [%d]", rb1.getRowNum(), rb2.getRowNum());
      }
      if (rb1.compareTo(rb2) != 0) {
        try {
          validateRowValues(dimHandlers, rb1, adapter1, rb2, adapter2);
        }
        catch (SegmentValidationException ex) {
          throw new SegmentValidationException(ex, "Validation failure on row %d: [%s] vs [%s]", row, rb1, rb2);
        }
      }
    }
    if (it2.hasNext()) {
      throw new SegmentValidationException("Unexpected end of first adapter");
    }
    if (row != adapter1.getNumRows()) {
      throw new SegmentValidationException(
          "Actual Row count mismatch. Expected [%d] found [%d]",
          row,
          adapter1.getNumRows()
      );
    }
  }

  public QueryableIndex loadIndex(File inDir) throws IOException
  {
    final int version = SegmentUtils.getVersionFromDir(inDir);

    final IndexLoader loader = indexLoaders.get(version);

    if (loader != null) {
      return loader.load(inDir, mapper);
    } else {
      throw new ISE("Unknown index version[%s]", version);
    }
  }

  public static int getVersionFromDir(File inDir) throws IOException
  {
    File versionFile = new File(inDir, "version.bin");
    if (versionFile.exists()) {
      return Ints.fromByteArray(Files.toByteArray(versionFile));
    }

    final File indexFile = new File(inDir, "index.drd");
    int version;
    try (InputStream in = new FileInputStream(indexFile)) {
      version = in.read();
    }
    return version;
  }

  public static void checkFileSize(File indexFile) throws IOException
  {
    final long fileSize = indexFile.length();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOException(String.format("File[%s] too large[%s]", indexFile, fileSize));
    }
  }

  public boolean convertSegment(File toConvert, File converted, IndexSpec indexSpec) throws IOException
  {
    return convertSegment(toConvert, converted, indexSpec, false, true);
  }

  public boolean convertSegment(
      File toConvert,
      File converted,
      IndexSpec indexSpec,
      boolean forceIfCurrent,
      boolean validate
  ) throws IOException
  {
    final int version = SegmentUtils.getVersionFromDir(toConvert);
    switch (version) {
      case 1:
      case 2:
      case 3:
        log.makeAlert("Attempt to load segment of version <= 3.")
           .addData("version", version)
           .emit();
        return false;
      case 4:
      case 5:
      case 6:
      case 7:
        log.info("Old version, re-persisting.");
        try (QueryableIndex segmentToConvert = loadIndex(toConvert)) {
          new IndexMerger(mapper, this).append(
              Arrays.<IndexableAdapter>asList(new QueryableIndexIndexableAdapter(segmentToConvert)),
              null,
              converted,
              indexSpec
          );
        }
        return true;
      case 8:
        defaultIndexIOHandler.convertV8toV9(toConvert, converted, indexSpec);
        return true;
      default:
        if (forceIfCurrent) {
          new IndexMerger(mapper, this).convert(toConvert, converted, indexSpec);
          if (validate) {
            validateTwoSegments(toConvert, converted);
          }
          return true;
        } else {
          log.info("Version[%s], skipping.", version);
          return false;
        }
    }
  }

  public DefaultIndexIOHandler getDefaultIndexIOHandler()
  {
    return defaultIndexIOHandler;
  }

  static interface IndexIOHandler
  {
    public MMappedIndex mapDir(File inDir) throws IOException;
  }

  public static void validateRowValues(
      Map<String, DimensionHandler> dimHandlers,
      Rowboat rb1,
      IndexableAdapter adapter1,
      Rowboat rb2,
      IndexableAdapter adapter2
  )
  {
    if (rb1.getTimestamp() != rb2.getTimestamp()) {
      throw new SegmentValidationException(
          "Timestamp mismatch. Expected %d found %d",
          rb1.getTimestamp(), rb2.getTimestamp()
      );
    }
    final Object[] dims1 = rb1.getDims();
    final Object[] dims2 = rb2.getDims();
    if (dims1.length != dims2.length) {
      throw new SegmentValidationException(
          "Dim lengths not equal %s vs %s",
          Arrays.deepToString(dims1),
          Arrays.deepToString(dims2)
      );
    }
    final Indexed<String> dim1Names = adapter1.getDimensionNames();
    final Indexed<String> dim2Names = adapter2.getDimensionNames();
    for (int i = 0; i < dims1.length; ++i) {
      final Object dim1Vals = dims1[i];
      final Object dim2Vals = dims2[i];
      final String dim1Name = dim1Names.get(i);
      final String dim2Name = dim2Names.get(i);

      ColumnCapabilities capabilities1 = adapter1.getCapabilities(dim1Name);
      ColumnCapabilities capabilities2 = adapter2.getCapabilities(dim2Name);
      ValueType dim1Type = capabilities1.getType();
      ValueType dim2Type = capabilities2.getType();
      if (dim1Type != dim2Type) {
        throw new SegmentValidationException(
            "Dim [%s] types not equal. Expected %d found %d",
            dim1Name,
            dim1Type,
            dim2Type
        );
      }

      DimensionHandler dimHandler = dimHandlers.get(dim1Name);
      dimHandler.validateSortedEncodedArrays(
          dim1Vals,
          dim2Vals,
          adapter1.getDimValueLookup(dim1Name),
          adapter2.getDimValueLookup(dim2Name)
      );
    }
  }

  public static class DefaultIndexIOHandler implements IndexIOHandler
  {
    private static final Logger log = new Logger(DefaultIndexIOHandler.class);
    private final ObjectMapper mapper;

    public DefaultIndexIOHandler(ObjectMapper mapper)
    {
      this.mapper = mapper;
    }

    @Override
    public MMappedIndex mapDir(File inDir) throws IOException
    {
      log.debug("Mapping v8 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(inDir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != V8_VERSION) {
          throw new IllegalArgumentException(String.format("Unknown version[%s]", theVersion));
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);
      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");

      indexBuffer.get(); // Skip the version byte
      final GenericIndexed<String> availableDimensions = GenericIndexed.read(
          indexBuffer, GenericIndexed.STRING_STRATEGY
      );
      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
          indexBuffer, GenericIndexed.STRING_STRATEGY
      );
      final Interval dataInterval = new Interval(serializerUtils.readString(indexBuffer));
      final BitmapSerdeFactory bitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();

      CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
          smooshedFiles.mapFile(makeTimeFile(inDir, BYTE_ORDER).getName()), BYTE_ORDER
      );

      Map<String, MetricHolder> metrics = Maps.newLinkedHashMap();
      for (String metric : availableMetrics) {
        final String metricFilename = makeMetricFile(inDir, metric, BYTE_ORDER).getName();
        final MetricHolder holder = MetricHolder.fromByteBuffer(smooshedFiles.mapFile(metricFilename));

        if (!metric.equals(holder.getName())) {
          throw new ISE("Metric[%s] loaded up metric[%s] from disk.  File names do matter.", metric, holder.getName());
        }
        metrics.put(metric, holder);
      }

      Map<String, GenericIndexed<String>> dimValueLookups = Maps.newHashMap();
      Map<String, VSizeIndexed> dimColumns = Maps.newHashMap();
      Map<String, GenericIndexed<ImmutableBitmap>> bitmaps = Maps.newHashMap();

      for (String dimension : IndexedIterable.create(availableDimensions)) {
        ByteBuffer dimBuffer = smooshedFiles.mapFile(makeDimFile(inDir, dimension).getName());
        String fileDimensionName = serializerUtils.readString(dimBuffer);
        Preconditions.checkState(
            dimension.equals(fileDimensionName),
            "Dimension file[%s] has dimension[%s] in it!?",
            makeDimFile(inDir, dimension),
            fileDimensionName
        );

        dimValueLookups.put(dimension, GenericIndexed.read(dimBuffer, GenericIndexed.STRING_STRATEGY));
        dimColumns.put(dimension, VSizeIndexed.readFromByteBuffer(dimBuffer));
      }

      ByteBuffer invertedBuffer = smooshedFiles.mapFile("inverted.drd");
      for (int i = 0; i < availableDimensions.size(); ++i) {
        bitmaps.put(
            serializerUtils.readString(invertedBuffer),
            GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
        );
      }

      Map<String, ImmutableRTree> spatialIndexed = Maps.newHashMap();
      ByteBuffer spatialBuffer = smooshedFiles.mapFile("spatial.drd");
      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
        spatialIndexed.put(
            serializerUtils.readString(spatialBuffer),
            ByteBufferSerializer.read(
                spatialBuffer,
                new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
            )
        );
      }

      final MMappedIndex retVal = new MMappedIndex(
          availableDimensions,
          availableMetrics,
          dataInterval,
          timestamps,
          metrics,
          dimValueLookups,
          dimColumns,
          bitmaps,
          spatialIndexed,
          smooshedFiles
      );

      log.debug("Mapped v8 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return retVal;
    }

    public void convertV8toV9(File v8Dir, File v9Dir, IndexSpec indexSpec)
        throws IOException
    {
      log.info("Converting v8[%s] to v9[%s]", v8Dir, v9Dir);

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(v8Dir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != V8_VERSION) {
          throw new IAE("Unknown version[%s]", theVersion);
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      Closer closer = Closer.create();
      try {
        SmooshedFileMapper v8SmooshedFiles = closer.register(Smoosh.map(v8Dir));

        v9Dir.mkdirs();
        final FileSmoosher v9Smoosher = closer.register(new FileSmoosher(v9Dir));

        ByteStreams.write(Ints.toByteArray(9), Files.newOutputStreamSupplier(new File(v9Dir, "version.bin")));

        Map<String, GenericIndexed<ImmutableBitmap>> bitmapIndexes = Maps.newHashMap();
        final ByteBuffer invertedBuffer = v8SmooshedFiles.mapFile("inverted.drd");
        BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

        while (invertedBuffer.hasRemaining()) {
          final String dimName = serializerUtils.readString(invertedBuffer);
          bitmapIndexes.put(
              dimName,
              GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
          );
        }

        Map<String, ImmutableRTree> spatialIndexes = Maps.newHashMap();
        final ByteBuffer spatialBuffer = v8SmooshedFiles.mapFile("spatial.drd");
        while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
          spatialIndexes.put(
              serializerUtils.readString(spatialBuffer),
              ByteBufferSerializer.read(
                  spatialBuffer, new IndexedRTree.ImmutableRTreeObjectStrategy(
                      bitmapSerdeFactory.getBitmapFactory()
                  )
              )
          );
        }

        final LinkedHashSet<String> skippedFiles = Sets.newLinkedHashSet();
        final Set<String> skippedDimensions = Sets.newLinkedHashSet();
        for (String filename : v8SmooshedFiles.getInternalFilenames()) {
          log.info("Processing file[%s]", filename);
          if (filename.startsWith("dim_")) {
            final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
            builder.setValueType(ValueType.STRING);

            final List<ByteBuffer> outParts = Lists.newArrayList();

            ByteBuffer dimBuffer = v8SmooshedFiles.mapFile(filename);
            String dimension = serializerUtils.readString(dimBuffer);
            if (!filename.equals(String.format("dim_%s.drd", dimension))) {
              throw new ISE("loaded dimension[%s] from file[%s]", dimension, filename);
            }

            ByteArrayOutputStream nameBAOS = new ByteArrayOutputStream();
            serializerUtils.writeString(nameBAOS, dimension);
            outParts.add(ByteBuffer.wrap(nameBAOS.toByteArray()));

            GenericIndexed<String> dictionary = GenericIndexed.read(
                dimBuffer, GenericIndexed.STRING_STRATEGY
            );

            if (dictionary.size() == 0) {
              log.info("Dimension[%s] had cardinality 0, equivalent to no column, so skipping.", dimension);
              skippedDimensions.add(dimension);
              continue;
            }

            int emptyStrIdx = dictionary.indexOf("");
            List<Integer> singleValCol = null;
            VSizeIndexed multiValCol = VSizeIndexed.readFromByteBuffer(dimBuffer.asReadOnlyBuffer());
            GenericIndexed<ImmutableBitmap> bitmaps = bitmapIndexes.get(dimension);
            ImmutableRTree spatialIndex = spatialIndexes.get(dimension);

            final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
            boolean onlyOneValue = true;
            MutableBitmap nullsSet = null;
            for (int i = 0; i < multiValCol.size(); ++i) {
              VSizeIndexedInts rowValue = multiValCol.get(i);
              if (!onlyOneValue) {
                break;
              }
              if (rowValue.size() > 1) {
                onlyOneValue = false;
              }
              if (rowValue.size() == 0 || rowValue.get(0) == emptyStrIdx) {
                if (nullsSet == null) {
                  nullsSet = bitmapFactory.makeEmptyMutableBitmap();
                }
                nullsSet.add(i);
              }
            }

            if (onlyOneValue) {
              log.info("Dimension[%s] is single value, converting...", dimension);
              final boolean bumpedDictionary;
              if (nullsSet != null) {
                log.info("Dimension[%s] has null rows.", dimension);
                final ImmutableBitmap theNullSet = bitmapFactory.makeImmutableBitmap(nullsSet);

                if (dictionary.get(0) != null) {
                  log.info("Dimension[%s] has no null value in the dictionary, expanding...", dimension);
                  bumpedDictionary = true;
                  final List<String> nullList = Lists.newArrayList();
                  nullList.add(null);

                  dictionary = GenericIndexed.fromIterable(
                      Iterables.concat(nullList, dictionary),
                      GenericIndexed.STRING_STRATEGY
                  );

                  bitmaps = GenericIndexed.fromIterable(
                      Iterables.concat(Arrays.asList(theNullSet), bitmaps),
                      bitmapSerdeFactory.getObjectStrategy()
                  );
                } else {
                  bumpedDictionary = false;
                  bitmaps = GenericIndexed.fromIterable(
                      Iterables.concat(
                          Arrays.asList(
                              bitmapFactory
                                  .union(Arrays.asList(theNullSet, bitmaps.get(0)))
                          ),
                          Iterables.skip(bitmaps, 1)
                      ),
                      bitmapSerdeFactory.getObjectStrategy()
                  );
                }
              } else {
                bumpedDictionary = false;
              }

              final VSizeIndexed finalMultiValCol = multiValCol;
              singleValCol = new AbstractList<Integer>()
              {
                @Override
                public Integer get(int index)
                {
                  final VSizeIndexedInts ints = finalMultiValCol.get(index);
                  return ints.size() == 0 ? 0 : ints.get(0) + (bumpedDictionary ? 1 : 0);
                }

                @Override
                public int size()
                {
                  return finalMultiValCol.size();
                }
              };

              multiValCol = null;
            } else {
              builder.setHasMultipleValues(true);
            }

            final CompressedObjectStrategy.CompressionStrategy compressionStrategy = indexSpec.getDimensionCompression();

            final DictionaryEncodedColumnPartSerde.LegacySerializerBuilder columnPartBuilder = DictionaryEncodedColumnPartSerde
                .legacySerializerBuilder()
                .withDictionary(dictionary)
                .withBitmapSerdeFactory(bitmapSerdeFactory)
                .withBitmaps(bitmaps)
                .withSpatialIndex(spatialIndex)
                .withByteOrder(BYTE_ORDER);

            if (singleValCol != null) {
              if (compressionStrategy != CompressedObjectStrategy.CompressionStrategy.UNCOMPRESSED) {
                columnPartBuilder.withSingleValuedColumn(
                    CompressedVSizeIntsIndexedSupplier.fromList(
                        singleValCol,
                        dictionary.size(),
                        CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(dictionary.size()),
                        BYTE_ORDER,
                        compressionStrategy
                    )
                );
              } else {
                columnPartBuilder.withSingleValuedColumn(VSizeIndexedInts.fromList(singleValCol, dictionary.size()));
              }
            } else if (compressionStrategy != CompressedObjectStrategy.CompressionStrategy.UNCOMPRESSED) {
              columnPartBuilder.withMultiValuedColumn(
                  CompressedVSizeIndexedSupplier.fromIterable(
                      multiValCol,
                      dictionary.size(),
                      BYTE_ORDER,
                      compressionStrategy
                  )
              );
            } else {
              columnPartBuilder.withMultiValuedColumn(multiValCol);
            }

            final ColumnDescriptor serdeficator = builder
                .addSerde(columnPartBuilder.build())
                .build();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
            byte[] specBytes = baos.toByteArray();

            final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
                dimension, serdeficator.numBytes() + specBytes.length
            );
            channel.write(ByteBuffer.wrap(specBytes));
            serdeficator.write(channel);
            channel.close();
          } else if (filename.startsWith("met_") || filename.startsWith("numeric_dim_")) {
            // NOTE: identifying numeric dimensions by using a different filename pattern is meant to allow the
            // legacy merger (which will be deprecated) to support long/float dims. Going forward, the V9 merger
            // should be used instead if any dimension types beyond String are needed.
            if (!filename.endsWith(String.format("%s.drd", BYTE_ORDER))) {
              skippedFiles.add(filename);
              continue;
            }

            MetricHolder holder = MetricHolder.fromByteBuffer(v8SmooshedFiles.mapFile(filename));
            final String metric = holder.getName();

            final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();

            switch (holder.getType()) {
              case LONG:
                builder.setValueType(ValueType.LONG);
                builder.addSerde(
                    LongGenericColumnPartSerde.legacySerializerBuilder()
                                              .withByteOrder(BYTE_ORDER)
                                              .withDelegate(holder.longType)
                                              .build()
                );
                break;
              case FLOAT:
                builder.setValueType(ValueType.FLOAT);
                builder.addSerde(
                    FloatGenericColumnPartSerde.legacySerializerBuilder()
                                               .withByteOrder(BYTE_ORDER)
                                               .withDelegate(holder.floatType)
                                               .build()
                );
                break;
              case COMPLEX:
                if (!(holder.complexType instanceof GenericIndexed)) {
                  throw new ISE("Serialized complex types must be GenericIndexed objects.");
                }
                final GenericIndexed column = (GenericIndexed) holder.complexType;
                final String complexType = holder.getTypeName();
                builder.setValueType(ValueType.COMPLEX);
                builder.addSerde(
                    ComplexColumnPartSerde.legacySerializerBuilder()
                                          .withTypeName(complexType)
                                          .withDelegate(column).build()
                );
                break;
              default:
                throw new ISE("Unknown type[%s]", holder.getType());
            }

            final ColumnDescriptor serdeficator = builder.build();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
            byte[] specBytes = baos.toByteArray();

            final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
                metric, serdeficator.numBytes() + specBytes.length
            );
            channel.write(ByteBuffer.wrap(specBytes));
            serdeficator.write(channel);
            channel.close();
          } else if (String.format("time_%s.drd", BYTE_ORDER).equals(filename)) {
            CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
                v8SmooshedFiles.mapFile(filename), BYTE_ORDER
            );

            final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
            builder.setValueType(ValueType.LONG);
            builder.addSerde(
                LongGenericColumnPartSerde.legacySerializerBuilder()
                                          .withByteOrder(BYTE_ORDER)
                                          .withDelegate(timestamps)
                                          .build()
            );
            final ColumnDescriptor serdeficator = builder.build();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
            byte[] specBytes = baos.toByteArray();

            final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
                "__time", serdeficator.numBytes() + specBytes.length
            );
            channel.write(ByteBuffer.wrap(specBytes));
            serdeficator.write(channel);
            channel.close();
          } else {
            skippedFiles.add(filename);
          }
        }

        final ByteBuffer indexBuffer = v8SmooshedFiles.mapFile("index.drd");

        indexBuffer.get(); // Skip the version byte
        final GenericIndexed<String> dims8 = GenericIndexed.read(
            indexBuffer, GenericIndexed.STRING_STRATEGY
        );
        final GenericIndexed<String> dims9 = GenericIndexed.fromIterable(
            Iterables.filter(
                dims8, new Predicate<String>()
                {
                  @Override
                  public boolean apply(String s)
                  {
                    return !skippedDimensions.contains(s);
                  }
                }
            ),
            GenericIndexed.STRING_STRATEGY
        );
        final GenericIndexed<String> availableMetrics = GenericIndexed.read(
            indexBuffer, GenericIndexed.STRING_STRATEGY
        );
        final Interval dataInterval = new Interval(serializerUtils.readString(indexBuffer));
        final BitmapSerdeFactory segmentBitmapSerdeFactory = mapper.readValue(
            serializerUtils.readString(indexBuffer),
            BitmapSerdeFactory.class
        );

        Set<String> columns = Sets.newTreeSet();
        columns.addAll(Lists.newArrayList(dims9));
        columns.addAll(Lists.newArrayList(availableMetrics));
        GenericIndexed<String> cols = GenericIndexed.fromIterable(columns, GenericIndexed.STRING_STRATEGY);

        final String segmentBitmapSerdeFactoryString = mapper.writeValueAsString(segmentBitmapSerdeFactory);

        final long numBytes = cols.getSerializedSize() + dims9.getSerializedSize() + 16
                              + serializerUtils.getSerializedStringByteSize(segmentBitmapSerdeFactoryString);
        final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);
        cols.writeToChannel(writer);
        dims9.writeToChannel(writer);
        serializerUtils.writeLong(writer, dataInterval.getStartMillis());
        serializerUtils.writeLong(writer, dataInterval.getEndMillis());
        serializerUtils.writeString(writer, segmentBitmapSerdeFactoryString);
        writer.close();

        final ByteBuffer metadataBuffer = v8SmooshedFiles.mapFile("metadata.drd");
        if (metadataBuffer != null) {
          v9Smoosher.add("metadata.drd", metadataBuffer);
        }

        log.info("Skipped files[%s]", skippedFiles);

      }
      catch (Throwable t) {
        throw closer.rethrow(t);
      }
      finally {
        closer.close();
      }
    }
  }

  static interface IndexLoader
  {
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException;
  }

  static class LegacyIndexLoader implements IndexLoader
  {
    private final IndexIOHandler legacyHandler;
    private final ColumnConfig columnConfig;

    LegacyIndexLoader(IndexIOHandler legacyHandler, ColumnConfig columnConfig)
    {
      this.legacyHandler = legacyHandler;
      this.columnConfig = columnConfig;
    }

    @Override
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException
    {
      MMappedIndex index = legacyHandler.mapDir(inDir);

      Map<String, Column> columns = Maps.newHashMap();

      for (String dimension : index.getAvailableDimensions()) {
        ColumnBuilder builder = new ColumnBuilder()
            .setType(ValueType.STRING)
            .setHasMultipleValues(true)
            .setDictionaryEncodedColumn(
                new DictionaryEncodedColumnSupplier(
                    index.getDimValueLookup(dimension),
                    null,
                    Suppliers.<IndexedMultivalue<IndexedInts>>ofInstance(
                        index.getDimColumn(dimension)
                    ),
                    columnConfig.columnCacheSizeBytes()
                )
            )
            .setBitmapIndex(
                new BitmapIndexColumnPartSupplier(
                    new ConciseBitmapFactory(),
                    index.getBitmapIndexes().get(dimension),
                    index.getDimValueLookup(dimension)
                )
            );
        if (index.getSpatialIndexes().get(dimension) != null) {
          builder.setSpatialIndex(
              new SpatialIndexColumnPartSupplier(
                  index.getSpatialIndexes().get(dimension)
              )
          );
        }
        columns.put(
            dimension,
            builder.build()
        );
      }

      for (String metric : index.getAvailableMetrics()) {
        final MetricHolder metricHolder = index.getMetricHolder(metric);
        if (metricHolder.getType() == MetricHolder.MetricType.FLOAT) {
          columns.put(
              metric,
              new ColumnBuilder()
                  .setType(ValueType.FLOAT)
                  .setGenericColumn(new FloatGenericColumnSupplier(metricHolder.floatType, BYTE_ORDER))
                  .build()
          );
        } else if (metricHolder.getType() == MetricHolder.MetricType.COMPLEX) {
          columns.put(
              metric,
              new ColumnBuilder()
                  .setType(ValueType.COMPLEX)
                  .setComplexColumn(
                      new ComplexColumnPartSupplier(
                          metricHolder.getTypeName(), (GenericIndexed) metricHolder.complexType
                      )
                  )
                  .build()
          );
        }
      }

      Set<String> colSet = Sets.newTreeSet();
      for (String dimension : index.getAvailableDimensions()) {
        colSet.add(dimension);
      }
      for (String metric : index.getAvailableMetrics()) {
        colSet.add(metric);
      }

      String[] cols = colSet.toArray(new String[colSet.size()]);
      columns.put(
          Column.TIME_COLUMN_NAME, new ColumnBuilder()
              .setType(ValueType.LONG)
              .setGenericColumn(new LongGenericColumnSupplier(index.timestamps))
              .build()
      );
      return new SimpleQueryableIndex(
          index.getDataInterval(),
          new ArrayIndexed<>(cols, String.class),
          index.getAvailableDimensions(),
          new ConciseBitmapFactory(),
          columns,
          index.getFileMapper(),
          null
      );
    }
  }

  static class V9IndexLoader implements IndexLoader
  {
    private final ColumnConfig columnConfig;

    V9IndexLoader(ColumnConfig columnConfig)
    {
      this.columnConfig = columnConfig;
    }

    @Override
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException
    {
      log.debug("Mapping v9 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      final int theVersion = Ints.fromByteArray(Files.toByteArray(new File(inDir, "version.bin")));
      if (theVersion != V9_VERSION) {
        throw new IllegalArgumentException(String.format("Expected version[9], got[%s]", theVersion));
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);

      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
      /**
       * Index.drd should consist of the segment version, the columns and dimensions of the segment as generic
       * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
       */
      final GenericIndexed<String> cols = GenericIndexed.read(indexBuffer, GenericIndexed.STRING_STRATEGY);
      final GenericIndexed<String> dims = GenericIndexed.read(indexBuffer, GenericIndexed.STRING_STRATEGY);
      final Interval dataInterval = new Interval(indexBuffer.getLong(), indexBuffer.getLong());
      final BitmapSerdeFactory segmentBitmapSerdeFactory;

      /**
       * This is a workaround for the fact that in v8 segments, we have no information about the type of bitmap
       * index to use. Since we cannot very cleanly build v9 segments directly, we are using a workaround where
       * this information is appended to the end of index.drd.
       */
      if (indexBuffer.hasRemaining()) {
        segmentBitmapSerdeFactory = mapper.readValue(serializerUtils.readString(indexBuffer), BitmapSerdeFactory.class);
      } else {
        segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
      }

      Metadata metadata = null;
      ByteBuffer metadataBB = smooshedFiles.mapFile("metadata.drd");
      if (metadataBB != null) {
        try {
          metadata = mapper.readValue(
              serializerUtils.readBytes(metadataBB, metadataBB.remaining()),
              Metadata.class
          );
        }
        catch (JsonParseException | JsonMappingException ex) {
          // Any jackson deserialization errors are ignored e.g. if metadata contains some aggregator which
          // is no longer supported then it is OK to not use the metadata instead of failing segment loading
          log.warn(ex, "Failed to load metadata for segment [%s]", inDir);
        }
        catch (IOException ex) {
          throw new IOException("Failed to read metadata", ex);
        }
      }

      Map<String, Column> columns = Maps.newHashMap();

      for (String columnName : cols) {
        columns.put(columnName, deserializeColumn(mapper, smooshedFiles.mapFile(columnName)));
      }

      columns.put(Column.TIME_COLUMN_NAME, deserializeColumn(mapper, smooshedFiles.mapFile("__time")));

      final QueryableIndex index = new SimpleQueryableIndex(
          dataInterval, cols, dims, segmentBitmapSerdeFactory.getBitmapFactory(), columns, smooshedFiles, metadata
      );

      log.debug("Mapped v9 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return index;
    }

    private Column deserializeColumn(ObjectMapper mapper, ByteBuffer byteBuffer) throws IOException
    {
      ColumnDescriptor serde = mapper.readValue(
          serializerUtils.readString(byteBuffer), ColumnDescriptor.class
      );
      return serde.read(byteBuffer, columnConfig);
    }
  }

  public static File makeDimFile(File dir, String dimension)
  {
    return new File(dir, String.format("dim_%s.drd", dimension));
  }

  public static File makeNumericDimFile(File dir, String dimension, ByteOrder order)
  {
    return new File(dir, String.format("numeric_dim_%s_%s.drd", dimension, order));
  }

  public static File makeTimeFile(File dir, ByteOrder order)
  {
    return new File(dir, String.format("time_%s.drd", order));
  }

  public static File makeMetricFile(File dir, String metricName, ByteOrder order)
  {
    return new File(dir, String.format("met_%s_%s.drd", metricName, order));
  }
}
