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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.RTree;
import com.metamx.collections.spatial.split.LinearGutmanSplitStrategy;
import com.metamx.common.ISE;
import com.metamx.common.io.smoosh.FileSmoosher;
import com.metamx.common.io.smoosh.SmooshedWriter;
import com.metamx.common.logger.Logger;
import io.druid.collections.CombiningIterable;
import io.druid.common.utils.JodaUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ColumnDescriptor;
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
import io.druid.segment.data.IndexedIntsWriter;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.data.VSizeIndexedIntsWriter;
import io.druid.segment.data.VSizeIndexedWriter;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnSerializer;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.FloatGenericColumnPartSerde;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class IndexMergerV9 extends IndexMerger
{
  private static final Logger log = new Logger(IndexMergerV9.class);

  @Inject
  public IndexMergerV9(
      ObjectMapper mapper,
      IndexIO indexIO
  )
  {
    super(mapper, indexIO);
  }

  @Override
  protected File makeIndexFiles(
      final List<IndexableAdapter> adapters,
      final AggregatorFactory[] metricAggs,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn,
      final IndexSpec indexSpec
  ) throws IOException
  {
    progress.start();
    progress.progress();

    List<Metadata> metadataList = Lists.transform(
        adapters,
        new Function<IndexableAdapter, Metadata>()
        {
          @Override
          public Metadata apply(IndexableAdapter input)
          {
            return input.getMetadata();
          }
        }
    );

    Metadata segmentMetadata = null;
    if (metricAggs != null) {
      AggregatorFactory[] combiningMetricAggs = new AggregatorFactory[metricAggs.length];
      for (int i = 0; i < metricAggs.length; i++) {
        combiningMetricAggs[i] = metricAggs[i].getCombiningFactory();
      }
      segmentMetadata = Metadata.merge(
          metadataList,
          combiningMetricAggs
      );
    } else {
      segmentMetadata = Metadata.merge(
          metadataList,
          null
      );
    }

    Closer closer = Closer.create();
    final IOPeon ioPeon = new TmpFileIOPeon(false);
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        ioPeon.cleanup();
      }
    });
    final FileSmoosher v9Smoosher = new FileSmoosher(outDir);
    final File v9TmpDir = new File(outDir, "v9-tmp");
    v9TmpDir.mkdirs();
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        FileUtils.deleteDirectory(v9TmpDir);
      }
    });
    log.info("Start making v9 index files, outDir:%s", outDir);
    try {
      long startTime = System.currentTimeMillis();
      ByteStreams.write(
          Ints.toByteArray(IndexIO.V9_VERSION),
          Files.newOutputStreamSupplier(new File(outDir, "version.bin"))
      );
      log.info("Completed version.bin in %,d millis.", System.currentTimeMillis() - startTime);

      progress.progress();
      final Map<String, ValueType> metricsValueTypes = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
      final Map<String, String> metricTypeNames = Maps.newTreeMap(Ordering.<String>natural().nullsFirst());
      final List<ColumnCapabilitiesImpl> dimCapabilities = Lists.newArrayListWithCapacity(mergedDimensions.size());
      mergeCapabilities(adapters, mergedDimensions, metricsValueTypes, metricTypeNames, dimCapabilities);

      /************* Setup Dim Conversions **************/
      progress.progress();
      startTime = System.currentTimeMillis();
      final Map<String, Integer> dimCardinalities = Maps.newHashMap();
      final ArrayList<GenericIndexedWriter<String>> dimValueWriters = setupDimValueWriters(ioPeon, mergedDimensions);
      final ArrayList<Map<String, IntBuffer>> dimConversions = Lists.newArrayListWithCapacity(adapters.size());
      final ArrayList<Boolean> dimensionSkipFlag = Lists.newArrayListWithCapacity(mergedDimensions.size());
      final ArrayList<Boolean> dimHasNullFlags = Lists.newArrayListWithCapacity(mergedDimensions.size());
      final ArrayList<Boolean> convertMissingDimsFlags = Lists.newArrayListWithCapacity(mergedDimensions.size());
      writeDimValueAndSetupDimConversion(
          adapters, progress, mergedDimensions, dimCardinalities, dimValueWriters, dimensionSkipFlag, dimConversions,
          convertMissingDimsFlags, dimHasNullFlags
      );
      log.info("Completed dim conversions in %,d millis.", System.currentTimeMillis() - startTime);

      /************* Walk through data sets, merge them, and write merged columns *************/
      progress.progress();
      final Iterable<Rowboat> theRows = makeRowIterable(
          adapters,
          mergedDimensions,
          mergedMetrics,
          dimConversions,
          convertMissingDimsFlags,
          rowMergerFn
      );
      final LongColumnSerializer timeWriter = setupTimeWriter(ioPeon);
      final ArrayList<IndexedIntsWriter> dimWriters = setupDimensionWriters(
          ioPeon, mergedDimensions, dimCapabilities, dimCardinalities, indexSpec
      );
      final ArrayList<GenericColumnSerializer> metWriters = setupMetricsWriters(
          ioPeon, mergedMetrics, metricsValueTypes, metricTypeNames, indexSpec
      );
      final List<IntBuffer> rowNumConversions = Lists.newArrayListWithCapacity(adapters.size());
      final ArrayList<MutableBitmap> nullRowsList = Lists.newArrayListWithCapacity(mergedDimensions.size());
      for (int i = 0; i < mergedDimensions.size(); ++i) {
        nullRowsList.add(indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap());
      }
      mergeIndexesAndWriteColumns(
          adapters, progress, theRows, timeWriter, dimWriters, metWriters,
          dimensionSkipFlag, rowNumConversions, nullRowsList, dimHasNullFlags
      );

      /************ Create Inverted Indexes *************/
      progress.progress();
      final ArrayList<GenericIndexedWriter<ImmutableBitmap>> bitmapIndexWriters = setupBitmapIndexWriters(
          ioPeon, mergedDimensions, indexSpec
      );
      final ArrayList<ByteBufferWriter<ImmutableRTree>> spatialIndexWriters = setupSpatialIndexWriters(
          ioPeon, mergedDimensions, indexSpec, dimCapabilities
      );
      makeInvertedIndexes(
          adapters, progress, mergedDimensions, indexSpec, v9TmpDir, rowNumConversions,
          nullRowsList, dimValueWriters, bitmapIndexWriters, spatialIndexWriters, dimConversions
      );

      /************ Finalize Build Columns *************/
      progress.progress();
      makeTimeColumn(v9Smoosher, progress, timeWriter);
      makeMetricsColumns(v9Smoosher, progress, mergedMetrics, metricsValueTypes, metricTypeNames, metWriters);
      makeDimensionColumns(
          v9Smoosher, progress, indexSpec, mergedDimensions, dimensionSkipFlag, dimCapabilities,
          dimValueWriters, dimWriters, bitmapIndexWriters, spatialIndexWriters
      );

      /************* Make index.drd & metadata.drd files **************/
      progress.progress();
      makeIndexBinary(
          v9Smoosher, adapters, outDir, mergedDimensions, dimensionSkipFlag, mergedMetrics, progress, indexSpec
      );
      makeMetadataBinary(v9Smoosher, progress, segmentMetadata);

      v9Smoosher.close();
      progress.stop();

      return outDir;
    }
    finally {
      closer.close();
    }
  }

  private void makeMetadataBinary(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final Metadata segmentMetadata
  ) throws IOException
  {
    if (segmentMetadata != null) {
      progress.startSection("make metadata.drd");
      v9Smoosher.add("metadata.drd", ByteBuffer.wrap(mapper.writeValueAsBytes(segmentMetadata)));
      progress.stopSection("make metadata.drd");
    }
  }

  private void makeIndexBinary(
      final FileSmoosher v9Smoosher,
      final List<IndexableAdapter> adapters,
      final File outDir,
      final List<String> mergedDimensions,
      final ArrayList<Boolean> dimensionSkipFlag,
      final List<String> mergedMetrics,
      final ProgressIndicator progress,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "make index.drd";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final Set<String> finalDimensions = Sets.newLinkedHashSet();
    final Set<String> finalColumns = Sets.newLinkedHashSet();
    finalColumns.addAll(mergedMetrics);
    for (int i = 0; i < mergedDimensions.size(); ++i) {
      if (dimensionSkipFlag.get(i)) {
        continue;
      }
      finalColumns.add(mergedDimensions.get(i));
      finalDimensions.add(mergedDimensions.get(i));
    }

    GenericIndexed<String> cols = GenericIndexed.fromIterable(finalColumns, GenericIndexed.STRING_STRATEGY);
    GenericIndexed<String> dims = GenericIndexed.fromIterable(finalDimensions, GenericIndexed.STRING_STRATEGY);

    final String bitmapSerdeFactoryType = mapper.writeValueAsString(indexSpec.getBitmapSerdeFactory());
    final long numBytes = cols.getSerializedSize()
                          + dims.getSerializedSize()
                          + 16
                          + serializerUtils.getSerializedStringByteSize(bitmapSerdeFactoryType);

    final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);
    cols.writeToChannel(writer);
    dims.writeToChannel(writer);

    DateTime minTime = new DateTime(JodaUtils.MAX_INSTANT);
    DateTime maxTime = new DateTime(JodaUtils.MIN_INSTANT);

    for (IndexableAdapter index : adapters) {
      minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
      maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
    }
    final Interval dataInterval = new Interval(minTime, maxTime);

    serializerUtils.writeLong(writer, dataInterval.getStartMillis());
    serializerUtils.writeLong(writer, dataInterval.getEndMillis());

    serializerUtils.writeString(
        writer, bitmapSerdeFactoryType
    );
    writer.close();

    IndexIO.checkFileSize(new File(outDir, "index.drd"));
    log.info("Completed index.drd in %,d millis.", System.currentTimeMillis() - startTime);

    progress.stopSection(section);
  }

  private void makeDimensionColumns(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final IndexSpec indexSpec,
      final List<String> mergedDimensions,
      final ArrayList<Boolean> dimensionSkipFlag,
      final List<ColumnCapabilitiesImpl> dimCapabilities,
      final ArrayList<GenericIndexedWriter<String>> dimValueWriters,
      final ArrayList<IndexedIntsWriter> dimWriters,
      final ArrayList<GenericIndexedWriter<ImmutableBitmap>> bitmapIndexWriters,
      final ArrayList<ByteBufferWriter<ImmutableRTree>> spatialIndexWriters
  ) throws IOException
  {
    final String section = "make dimension columns";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
    final CompressedObjectStrategy.CompressionStrategy compressionStrategy = indexSpec.getDimensionCompressionStrategy();
    for (int i = 0; i < mergedDimensions.size(); ++i) {
      long dimStartTime = System.currentTimeMillis();
      final String dim = mergedDimensions.get(i);
      final IndexedIntsWriter dimWriter = dimWriters.get(i);
      final GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter = bitmapIndexWriters.get(i);
      final ByteBufferWriter<ImmutableRTree> spatialIndexWriter = spatialIndexWriters.get(i);

      dimWriter.close();
      bitmapIndexWriter.close();
      if (spatialIndexWriter != null) {
        spatialIndexWriter.close();
      }
      if (dimensionSkipFlag.get(i)) {
        continue;
      }

      boolean hasMultiValue = dimCapabilities.get(i).hasMultipleValues();

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
      builder.setValueType(ValueType.STRING);
      builder.setHasMultipleValues(hasMultiValue);
      final DictionaryEncodedColumnPartSerde.SerializerBuilder partBuilder = DictionaryEncodedColumnPartSerde
          .serializerBuilder()
          .withDictionary(dimValueWriters.get(i))
          .withValue(dimWriters.get(i), hasMultiValue, compressionStrategy != null)
          .withBitmapSerdeFactory(bitmapSerdeFactory)
          .withBitmapIndex(bitmapIndexWriters.get(i))
          .withSpatialIndex(spatialIndexWriters.get(i))
          .withByteOrder(IndexIO.BYTE_ORDER);
      final ColumnDescriptor serdeficator = builder
          .addSerde(partBuilder.build())
          .build();
      makeColumn(v9Smoosher, dim, serdeficator);
      log.info("Completed dimension column[%s] in %,d millis.", dim, System.currentTimeMillis() - dimStartTime);
    }
    log.info("Completed dimension columns in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private void makeMetricsColumns(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final List<String> mergedMetrics,
      final Map<String, ValueType> metricsValueTypes,
      final Map<String, String> metricTypeNames,
      final List<GenericColumnSerializer> metWriters
  ) throws IOException
  {
    final String section = "make metric columns";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < mergedMetrics.size(); ++i) {
      String metric = mergedMetrics.get(i);
      long metricStartTime = System.currentTimeMillis();
      GenericColumnSerializer writer = metWriters.get(i);
      writer.close();

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
      ValueType type = metricsValueTypes.get(metric);
      switch (type) {
        case LONG:
          builder.setValueType(ValueType.LONG);
          builder.addSerde(
              LongGenericColumnPartSerde.serializerBuilder()
                                        .withByteOrder(IndexIO.BYTE_ORDER)
                                        .withDelegate((LongColumnSerializer) writer)
                                        .build()
          );
          break;
        case FLOAT:
          builder.setValueType(ValueType.FLOAT);
          builder.addSerde(
              FloatGenericColumnPartSerde.serializerBuilder()
                                         .withByteOrder(IndexIO.BYTE_ORDER)
                                         .withDelegate((FloatColumnSerializer) writer)
                                         .build()
          );
          break;
        case COMPLEX:
          final String typeName = metricTypeNames.get(metric);
          builder.setValueType(ValueType.COMPLEX);
          builder.addSerde(
              ComplexColumnPartSerde.serializerBuilder().withTypeName(typeName)
                                    .withDelegate((ComplexColumnSerializer) writer)
                                    .build()
          );
          break;
        default:
          throw new ISE("Unknown type[%s]", type);
      }
      makeColumn(v9Smoosher, metric, builder.build());
      log.info("Completed metric column[%s] in %,d millis.", metric, System.currentTimeMillis() - metricStartTime);
    }
    log.info("Completed metric columns in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }


  private void makeTimeColumn(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final LongColumnSerializer timeWriter
  ) throws IOException
  {
    final String section = "make time column";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    timeWriter.close();

    final ColumnDescriptor serdeficator = ColumnDescriptor
        .builder()
        .setValueType(ValueType.LONG)
        .addSerde(
            LongGenericColumnPartSerde.serializerBuilder()
                                      .withByteOrder(IndexIO.BYTE_ORDER)
                                      .withDelegate(timeWriter)
                                      .build()
        )
        .build();
    makeColumn(v9Smoosher, Column.TIME_COLUMN_NAME, serdeficator);
    log.info("Completed time column in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private void makeColumn(
      final FileSmoosher v9Smoosher,
      final String columnName,
      final ColumnDescriptor serdeficator
  ) throws IOException
  {
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

  private void makeInvertedIndexes(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final IndexSpec indexSpec,
      final File v9OutDir,
      final List<IntBuffer> rowNumConversions,
      final ArrayList<MutableBitmap> nullRowsList,
      final ArrayList<GenericIndexedWriter<String>> dimValueWriters,
      final ArrayList<GenericIndexedWriter<ImmutableBitmap>> bitmapIndexWriters,
      final ArrayList<ByteBufferWriter<ImmutableRTree>> spatialIndexWriters,
      final ArrayList<Map<String, IntBuffer>> dimConversions
  ) throws IOException
  {
    final String section = "build inverted index";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      String dimension = mergedDimensions.get(dimIndex);
      long dimStartTime = System.currentTimeMillis();

      // write dim values to one single file because we need to read it
      File dimValueFile = IndexIO.makeDimFile(v9OutDir, dimension);
      FileOutputStream fos = new FileOutputStream(dimValueFile);
      ByteStreams.copy(dimValueWriters.get(dimIndex).combineStreams(), fos);
      fos.close();

      final MappedByteBuffer dimValsMapped = Files.map(dimValueFile);
      Indexed<String> dimVals = GenericIndexed.read(dimValsMapped, GenericIndexed.STRING_STRATEGY);

      ByteBufferWriter<ImmutableRTree> spatialIndexWriter = spatialIndexWriters.get(dimIndex);
      RTree tree = null;
      if (spatialIndexWriter != null) {
        BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
        tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bitmapFactory), bitmapFactory);
      }

      IndexSeeker[] dictIdSeeker = toIndexSeekers(adapters, dimConversions, dimension);

      ImmutableBitmap nullRowBitmap = bitmapSerdeFactory.getBitmapFactory().makeImmutableBitmap(
          nullRowsList.get(dimIndex)
      );

      //Iterate all dim values's dictionary id in ascending order which in line with dim values's compare result.
      for (int dictId = 0; dictId < dimVals.size(); dictId++) {
        progress.progress();
        List<Iterable<Integer>> convertedInverteds = Lists.newArrayListWithCapacity(adapters.size());
        for (int j = 0; j < adapters.size(); ++j) {
          int seekedDictId = dictIdSeeker[j].seek(dictId);
          if (seekedDictId != IndexSeeker.NOT_EXIST) {
            convertedInverteds.add(
                new ConvertingIndexedInts(
                    adapters.get(j).getBitmapIndex(dimension, seekedDictId), rowNumConversions.get(j)
                )
            );
          }
        }

        MutableBitmap bitset = bitmapSerdeFactory.getBitmapFactory().makeEmptyMutableBitmap();
        for (Integer row : CombiningIterable.createSplatted(
            convertedInverteds,
            Ordering.<Integer>natural().nullsFirst()
        )) {
          if (row != INVALID_ROW) {
            bitset.add(row);
          }
        }

        ImmutableBitmap bitmapToWrite = bitmapSerdeFactory.getBitmapFactory().makeImmutableBitmap(bitset);
        if ((dictId == 0) && (Iterables.getFirst(dimVals, "") == null)) {
          bitmapToWrite = nullRowBitmap.union(bitmapToWrite);
        }
        bitmapIndexWriters.get(dimIndex).write(bitmapToWrite);

        if (spatialIndexWriter != null) {
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
      if (spatialIndexWriter != null) {
        spatialIndexWriter.write(ImmutableRTree.newImmutableFromMutable(tree));
      }
      log.info(
          "Completed dim[%s] inverted with cardinality[%,d] in %,d millis.",
          dimension,
          dimVals.size(),
          System.currentTimeMillis() - dimStartTime
      );
    }
    log.info("Completed inverted index in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }


  private ArrayList<GenericIndexedWriter<ImmutableBitmap>> setupBitmapIndexWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions,
      final IndexSpec indexSpec
  ) throws IOException
  {
    ArrayList<GenericIndexedWriter<ImmutableBitmap>> writers = Lists.newArrayListWithCapacity(mergedDimensions.size());
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
    for (String dimension : mergedDimensions) {
      GenericIndexedWriter<ImmutableBitmap> writer = new GenericIndexedWriter<>(
          ioPeon, String.format("%s.inverted", dimension), bitmapSerdeFactory.getObjectStrategy()
      );
      writer.open();
      writers.add(writer);
    }
    return writers;
  }

  private ArrayList<ByteBufferWriter<ImmutableRTree>> setupSpatialIndexWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions,
      final IndexSpec indexSpec,
      final List<ColumnCapabilitiesImpl> dimCapabilities
  ) throws IOException
  {
    ArrayList<ByteBufferWriter<ImmutableRTree>> writers = Lists.newArrayListWithCapacity(mergedDimensions.size());
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      if (dimCapabilities.get(dimIndex).hasSpatialIndexes()) {
        BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
        ByteBufferWriter<ImmutableRTree> writer = new ByteBufferWriter<>(
            ioPeon,
            String.format("%s.spatial", mergedDimensions.get(dimIndex)),
            new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapFactory)
        );
        writer.open();
        writers.add(writer);
      } else {
        writers.add(null);
      }
    }
    return writers;
  }

  private void mergeIndexesAndWriteColumns(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final Iterable<Rowboat> theRows,
      final LongColumnSerializer timeWriter,
      final ArrayList<IndexedIntsWriter> dimWriters,
      final ArrayList<GenericColumnSerializer> metWriters,
      final ArrayList<Boolean> dimensionSkipFlag,
      final List<IntBuffer> rowNumConversions,
      final ArrayList<MutableBitmap> nullRowsList,
      final ArrayList<Boolean> dimHasNullFlags
  ) throws IOException
  {
    final String section = "walk through and merge rows";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    int rowCount = 0;
    for (IndexableAdapter adapter : adapters) {
      int[] arr = new int[adapter.getNumRows()];
      Arrays.fill(arr, INVALID_ROW);
      rowNumConversions.add(IntBuffer.wrap(arr));
    }

    long time = System.currentTimeMillis();
    for (Rowboat theRow : theRows) {
      progress.progress();
      timeWriter.serialize(theRow.getTimestamp());

      final Object[] metrics = theRow.getMetrics();
      for (int i = 0; i < metrics.length; ++i) {
        metWriters.get(i).serialize(metrics[i]);
      }

      int[][] dims = theRow.getDims();
      for (int i = 0; i < dims.length; ++i) {
        if (dimensionSkipFlag.get(i)) {
          continue;
        }
        if (dims[i] == null || dims[i].length == 0) {
          nullRowsList.get(i).add(rowCount);
        } else if (dimHasNullFlags.get(i) && dims[i].length == 1 && dims[i][0] == 0) {
          // If this dimension has the null/empty str in its dictionary, a row with a single-valued dimension
          // that matches the null/empty str's dictionary ID should also be added to nullRowsList.
          nullRowsList.get(i).add(rowCount);
        }
        dimWriters.get(i).add(dims[i]);
      }

      for (Map.Entry<Integer, TreeSet<Integer>> comprisedRow : theRow.getComprisedRows().entrySet()) {
        final IntBuffer conversionBuffer = rowNumConversions.get(comprisedRow.getKey());

        for (Integer rowNum : comprisedRow.getValue()) {
          while (conversionBuffer.position() < rowNum) {
            conversionBuffer.put(INVALID_ROW);
          }
          conversionBuffer.put(rowCount);
        }
      }
      if ((++rowCount % 500000) == 0) {
        log.info("walked 500,000/%d rows in %,d millis.", rowCount, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
      }
    }
    for (IntBuffer rowNumConversion : rowNumConversions) {
      rowNumConversion.rewind();
    }
    log.info("completed walk through of %,d rows in %,d millis.", rowCount, System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private LongColumnSerializer setupTimeWriter(final IOPeon ioPeon) throws IOException
  {
    LongColumnSerializer timeWriter = LongColumnSerializer.create(
        ioPeon, "little_end_time", CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY
    );
    // we will close this writer after we added all the timestamps
    timeWriter.open();
    return timeWriter;
  }

  private ArrayList<GenericColumnSerializer> setupMetricsWriters(
      final IOPeon ioPeon,
      final List<String> mergedMetrics,
      final Map<String, ValueType> metricsValueTypes,
      final Map<String, String> metricTypeNames,
      final IndexSpec indexSpec
  ) throws IOException
  {
    ArrayList<GenericColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());
    final CompressedObjectStrategy.CompressionStrategy metCompression = indexSpec.getMetricCompressionStrategy();
    for (String metric : mergedMetrics) {
      ValueType type = metricsValueTypes.get(metric);
      GenericColumnSerializer writer;
      switch (type) {
        case LONG:
          writer = LongColumnSerializer.create(ioPeon, metric, metCompression);
          break;
        case FLOAT:
          writer = FloatColumnSerializer.create(ioPeon, metric, metCompression);
          break;
        case COMPLEX:
          final String typeName = metricTypeNames.get(metric);
          ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Unknown type[%s]", typeName);
          }
          writer = ComplexColumnSerializer.create(ioPeon, metric, serde);
          break;
        default:
          throw new ISE("Unknown type[%s]", type);
      }
      writer.open();
      // we will close these writers in another method after we added all the metrics
      metWriters.add(writer);
    }
    return metWriters;
  }

  private ArrayList<IndexedIntsWriter> setupDimensionWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions,
      final List<ColumnCapabilitiesImpl> dimCapabilities,
      final Map<String, Integer> dimCardinalities,
      final IndexSpec indexSpec
  ) throws IOException
  {
    ArrayList<IndexedIntsWriter> dimWriters = Lists.newArrayListWithCapacity(mergedDimensions.size());
    final CompressedObjectStrategy.CompressionStrategy dimCompression = indexSpec.getDimensionCompressionStrategy();
    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      String dim = mergedDimensions.get(dimIndex);
      int cardinality = dimCardinalities.get(dim);
      ColumnCapabilitiesImpl capabilities = dimCapabilities.get(dimIndex);
      String filenameBase = String.format("%s.forward_dim", dim);
      IndexedIntsWriter writer;
      if (capabilities.hasMultipleValues()) {
        writer = (dimCompression != null)
                 ? CompressedVSizeIndexedV3Writer.create(ioPeon, filenameBase, cardinality, dimCompression)
                 : new VSizeIndexedWriter(ioPeon, filenameBase, cardinality);
      } else {
        writer = (dimCompression != null)
                 ? CompressedVSizeIntsIndexedWriter.create(ioPeon, filenameBase, cardinality, dimCompression)
                 : new VSizeIndexedIntsWriter(ioPeon, filenameBase, cardinality);
      }
      writer.open();
      // we will close these writers in another method after we added all the values
      dimWriters.add(writer);
    }
    return dimWriters;
  }

  private ArrayList<GenericIndexedWriter<String>> setupDimValueWriters(
      final IOPeon ioPeon,
      final List<String> mergedDimensions
  )
      throws IOException
  {
    ArrayList<GenericIndexedWriter<String>> dimValueWriters = Lists.newArrayListWithCapacity(mergedDimensions.size());
    for (String dimension : mergedDimensions) {
      final GenericIndexedWriter<String> writer = new GenericIndexedWriter<>(
          ioPeon, String.format("%s.dim_values", dimension), GenericIndexed.STRING_STRATEGY
      );
      writer.open();
      dimValueWriters.add(writer);
    }
    return dimValueWriters;
  }

  private void writeDimValueAndSetupDimConversion(
      final List<IndexableAdapter> indexes,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final Map<String, Integer> dimensionCardinalities,
      final ArrayList<GenericIndexedWriter<String>> dimValueWriters,
      final ArrayList<Boolean> dimensionSkipFlag,
      final List<Map<String, IntBuffer>> dimConversions,
      final ArrayList<Boolean> convertMissingDimsFlags,
      final ArrayList<Boolean> dimHasNullFlags
  ) throws IOException
  {
    final String section = "setup dimension conversions";
    progress.startSection(section);

    for (int i = 0; i < indexes.size(); ++i) {
      dimConversions.add(Maps.<String, IntBuffer>newHashMap());
    }

    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      long dimStartTime = System.currentTimeMillis();
      String dimension = mergedDimensions.get(dimIndex);
      boolean dimHasNull = false;
      boolean dimHasValues = false;
      boolean dimAbsentFromSomeIndex = false;

      int numMergeIndex = 0;
      Indexed<String> dimValueLookup = null;
      Indexed<String>[] dimValueLookups = new Indexed[indexes.size() + 1];
      for (int i = 0; i < indexes.size(); i++) {
        Indexed<String> dimValues = indexes.get(i).getDimValueLookup(dimension);
        if (!isNullColumn(dimValues)) {
          dimHasValues = true;
          dimHasNull |= dimValues.indexOf(null) >= 0;
          dimValueLookups[i] = dimValueLookup = dimValues;
          numMergeIndex++;
        } else {
          dimAbsentFromSomeIndex = true;
        }
      }

      boolean convertMissingDims = dimHasValues && dimAbsentFromSomeIndex;
      convertMissingDimsFlags.add(convertMissingDims);

      /*
       * Ensure the empty str is always in the dictionary if the dimension was missing from one index but
       * has non-null values in another index.
       * This is done so that MMappedIndexRowIterable can convert null columns to empty strings
       * later on, to allow rows from indexes without a particular dimension to merge correctly with
       * rows from indexes with null/empty str values for that dimension.
       */
      if (convertMissingDims && !dimHasNull) {
        dimHasNull = true;
        dimValueLookups[indexes.size()] = dimValueLookup = EMPTY_STR_DIM_VAL;
        numMergeIndex++;
      }

      GenericIndexedWriter<String> writer = dimValueWriters.get(dimIndex);

      int cardinality = 0;
      if (numMergeIndex > 1) {
        DictionaryMergeIterator iterator = new DictionaryMergeIterator(dimValueLookups, true);

        while (iterator.hasNext()) {
          writer.write(iterator.next());
        }

        for (int i = 0; i < indexes.size(); i++) {
          if (dimValueLookups[i] != null && iterator.needConversion(i)) {
            dimConversions.get(i).put(dimension, iterator.conversions[i]);
          }
        }
        cardinality = iterator.counter;
      } else if (numMergeIndex == 1) {
        for (String value : dimValueLookup) {
          writer.write(value);
        }
        cardinality = dimValueLookup.size();
      }

      // Mark if this dim has the null/empty str value in its dictionary, used for determining nullRowsList later.
      dimHasNullFlags.add(dimHasNull);

      log.info(
          "Completed dim[%s] conversions with cardinality[%,d] in %,d millis.",
          dimension,
          cardinality,
          System.currentTimeMillis() - dimStartTime
      );
      dimensionCardinalities.put(dimension, cardinality);
      writer.close();

      if (cardinality == 0) {
        log.info(String.format("Skipping [%s], it is empty!", dimension));
        dimensionSkipFlag.add(true);
        continue;
      }
      dimensionSkipFlag.add(false);
    }
    progress.stopSection(section);
  }

  private void mergeCapabilities(
      final List<IndexableAdapter> adapters,
      final List<String> mergedDimensions,
      final Map<String, ValueType> metricsValueTypes,
      final Map<String, String> metricTypeNames,
      final List<ColumnCapabilitiesImpl> dimCapabilities
  )
  {
    final Map<String, ColumnCapabilitiesImpl> capabilitiesMap = Maps.newHashMap();
    for (IndexableAdapter adapter : adapters) {
      for (String dimension : adapter.getDimensionNames()) {
        ColumnCapabilitiesImpl mergedCapabilities = capabilitiesMap.get(dimension);
        if (mergedCapabilities == null) {
          mergedCapabilities = new ColumnCapabilitiesImpl();
          mergedCapabilities.setType(ValueType.STRING);
        }
        capabilitiesMap.put(dimension, mergedCapabilities.merge(adapter.getCapabilities(dimension)));
      }
      for (String metric : adapter.getMetricNames()) {
        ColumnCapabilitiesImpl mergedCapabilities = capabilitiesMap.get(metric);
        ColumnCapabilities capabilities = adapter.getCapabilities(metric);
        if (mergedCapabilities == null) {
          mergedCapabilities = new ColumnCapabilitiesImpl();
        }
        capabilitiesMap.put(metric, mergedCapabilities.merge(capabilities));
        metricsValueTypes.put(metric, capabilities.getType());
        metricTypeNames.put(metric, adapter.getMetricType(metric));
      }
    }
    for (String dim : mergedDimensions) {
      dimCapabilities.add(capabilitiesMap.get(dim));
    }
  }
}
