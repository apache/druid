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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.druid.collections.CombiningIterable;
import io.druid.common.utils.JodaUtils;
import io.druid.io.ZeroCopyByteArrayOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.MergeIterable;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.DoubleGenericColumnPartSerde;
import io.druid.segment.serde.FloatGenericColumnPartSerde;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexMergerV9 implements IndexMerger
{
  private static final Logger log = new Logger(IndexMergerV9.class);
  protected final ObjectMapper mapper;
  protected final IndexIO indexIO;

  @Inject
  public IndexMergerV9(
      ObjectMapper mapper,
      IndexIO indexIO
  )
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");

  }

  private static void registerDeleteDirectory(Closer closer, final File dir)
  {
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        FileUtils.deleteDirectory(dir);
      }
    });
  }

  private File makeIndexFiles(
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
    try {
      final FileSmoosher v9Smoosher = new FileSmoosher(outDir);
      final File v9TmpDir = new File(outDir, "v9-tmp");
      FileUtils.forceMkdir(v9TmpDir);
      registerDeleteDirectory(closer, v9TmpDir);
      log.info("Start making v9 index files, outDir:%s", outDir);

      File tmpPeonFilesDir = new File(v9TmpDir, "tmpPeonFiles");
      FileUtils.forceMkdir(tmpPeonFilesDir);
      registerDeleteDirectory(closer, tmpPeonFilesDir);
      final IOPeon ioPeon = new TmpFileIOPeon(tmpPeonFilesDir, false);
      closer.register(ioPeon);
      long startTime = System.currentTimeMillis();
      ByteStreams.write(
          Ints.toByteArray(IndexIO.V9_VERSION),
          Files.newOutputStreamSupplier(new File(outDir, "version.bin"))
      );
      log.info("Completed version.bin in %,d millis.", System.currentTimeMillis() - startTime);

      progress.progress();
      startTime = System.currentTimeMillis();
      try (FileOutputStream fos = new FileOutputStream(new File(outDir, "factory.json"))) {
        mapper.writeValue(fos, new MMappedQueryableSegmentizerFactory(indexIO));
      }
      log.info("Completed factory.json in %,d millis", System.currentTimeMillis() - startTime);

      progress.progress();
      final Map<String, ValueType> metricsValueTypes = Maps.newTreeMap(Comparators.<String>naturalNullsFirst());
      final Map<String, String> metricTypeNames = Maps.newTreeMap(Comparators.<String>naturalNullsFirst());
      final List<ColumnCapabilitiesImpl> dimCapabilities = Lists.newArrayListWithCapacity(mergedDimensions.size());
      mergeCapabilities(adapters, mergedDimensions, metricsValueTypes, metricTypeNames, dimCapabilities);

      final DimensionHandler[] handlers = makeDimensionHandlers(mergedDimensions, dimCapabilities);
      final List<DimensionMerger> mergers = new ArrayList<>();
      for (int i = 0; i < mergedDimensions.size(); i++) {
        mergers.add(handlers[i].makeMerger(indexSpec, v9TmpDir, ioPeon, dimCapabilities.get(i), progress));
      }

      /************* Setup Dim Conversions **************/
      progress.progress();
      startTime = System.currentTimeMillis();
      final ArrayList<Map<String, IntBuffer>> dimConversions = Lists.newArrayListWithCapacity(adapters.size());
      final ArrayList<Boolean> dimensionSkipFlag = Lists.newArrayListWithCapacity(mergedDimensions.size());
      final ArrayList<Boolean> convertMissingDimsFlags = Lists.newArrayListWithCapacity(mergedDimensions.size());
      writeDimValueAndSetupDimConversion(
          adapters, progress, mergedDimensions, mergers
      );
      log.info("Completed dim conversions in %,d millis.", System.currentTimeMillis() - startTime);

      /************* Walk through data sets, merge them, and write merged columns *************/
      progress.progress();
      final Iterable<Rowboat> theRows = makeRowIterable(
          adapters,
          mergedDimensions,
          mergedMetrics,
          rowMergerFn,
          dimCapabilities,
          handlers,
          mergers
      );
      final LongColumnSerializer timeWriter = setupTimeWriter(ioPeon, indexSpec);
      final ArrayList<GenericColumnSerializer> metWriters = setupMetricsWriters(
          ioPeon, mergedMetrics, metricsValueTypes, metricTypeNames, indexSpec
      );
      final List<IntBuffer> rowNumConversions = Lists.newArrayListWithCapacity(adapters.size());

      mergeIndexesAndWriteColumns(
          adapters, progress, theRows, timeWriter, metWriters, rowNumConversions, mergers
      );

      /************ Create Inverted Indexes and Finalize Build Columns *************/
      final String section = "build inverted index and columns";
      progress.startSection(section);
      makeTimeColumn(v9Smoosher, progress, timeWriter);
      makeMetricsColumns(v9Smoosher, progress, mergedMetrics, metricsValueTypes, metricTypeNames, metWriters);

      for (int i = 0; i < mergedDimensions.size(); i++) {
        DimensionMergerV9 merger = (DimensionMergerV9) mergers.get(i);
        merger.writeIndexes(rowNumConversions, closer);
        if (merger.canSkip()) {
          continue;
        }
        ColumnDescriptor columnDesc = merger.makeColumnDescriptor();
        makeColumn(v9Smoosher, mergedDimensions.get(i), columnDesc);
      }

      progress.stopSection(section);

      /************* Make index.drd & metadata.drd files **************/
      progress.progress();
      makeIndexBinary(
          v9Smoosher, adapters, outDir, mergedDimensions, mergedMetrics, progress, indexSpec, mergers
      );
      makeMetadataBinary(v9Smoosher, progress, segmentMetadata);

      v9Smoosher.close();
      progress.stop();

      return outDir;
    }
    catch (Throwable t) {
      throw closer.rethrow(t);
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
      final List<String> mergedMetrics,
      final ProgressIndicator progress,
      final IndexSpec indexSpec,
      final List<DimensionMerger> mergers
  ) throws IOException
  {
    final String section = "make index.drd";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final Set<String> finalDimensions = Sets.newLinkedHashSet();
    final Set<String> finalColumns = Sets.newLinkedHashSet();
    finalColumns.addAll(mergedMetrics);
    for (int i = 0; i < mergedDimensions.size(); ++i) {
      if (mergers.get(i).canSkip()) {
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
              LongGenericColumnPartSerde
                  .serializerBuilder()
                  .withByteOrder(IndexIO.BYTE_ORDER)
                  .withDelegate((LongColumnSerializer) writer)
                  .build()
          );
          break;
        case FLOAT:
          builder.setValueType(ValueType.FLOAT);
          builder.addSerde(
              FloatGenericColumnPartSerde
                  .serializerBuilder()
                  .withByteOrder(IndexIO.BYTE_ORDER)
                  .withDelegate((FloatColumnSerializer) writer)
                  .build()
          );
          break;
        case DOUBLE:
          builder.setValueType(ValueType.DOUBLE);
          builder.addSerde(
              DoubleGenericColumnPartSerde
                  .serializerBuilder()
                  .withByteOrder(IndexIO.BYTE_ORDER)
                  .withDelegate((DoubleColumnSerializer) writer)
                  .build()
          );
          break;
        case COMPLEX:
          final String typeName = metricTypeNames.get(metric);
          builder.setValueType(ValueType.COMPLEX);
          builder.addSerde(
              ComplexColumnPartSerde
                  .serializerBuilder()
                  .withTypeName(typeName)
                  .withDelegate(writer)
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
    ZeroCopyByteArrayOutputStream specBytes = new ZeroCopyByteArrayOutputStream();
    serializerUtils.writeString(specBytes, mapper.writeValueAsString(serdeficator));
    try (SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
        columnName, serdeficator.numBytes() + specBytes.size()
    )) {
      specBytes.writeTo(channel);
      serdeficator.write(channel, v9Smoosher);
    }
  }

  private void mergeIndexesAndWriteColumns(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final Iterable<Rowboat> theRows,
      final LongColumnSerializer timeWriter,
      final ArrayList<GenericColumnSerializer> metWriters,
      final List<IntBuffer> rowNumConversions,
      final List<DimensionMerger> mergers
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

      Object[] dims = theRow.getDims();
      for (int i = 0; i < dims.length; ++i) {
        DimensionMerger merger = mergers.get(i);
        if (merger.canSkip()) {
          continue;
        }
        merger.processMergedRow(dims[i]);
      }

      Iterator<Int2ObjectMap.Entry<IntSortedSet>> rowsIterator = theRow.getComprisedRows().int2ObjectEntrySet().fastIterator();
      while (rowsIterator.hasNext()) {
        Int2ObjectMap.Entry<IntSortedSet> comprisedRow = rowsIterator.next();

        final IntBuffer conversionBuffer = rowNumConversions.get(comprisedRow.getIntKey());

        for (IntIterator setIterator = comprisedRow.getValue().iterator(); setIterator.hasNext(); /* NOP */) {
          int rowNum = setIterator.nextInt();
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

  private LongColumnSerializer setupTimeWriter(final IOPeon ioPeon, final IndexSpec indexSpec) throws IOException
  {
    LongColumnSerializer timeWriter = LongColumnSerializer.create(
        ioPeon, "little_end_time", CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY,
        indexSpec.getLongEncoding()
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
    final CompressedObjectStrategy.CompressionStrategy metCompression = indexSpec.getMetricCompression();
    final CompressionFactory.LongEncodingStrategy longEncoding = indexSpec.getLongEncoding();
    for (String metric : mergedMetrics) {
      ValueType type = metricsValueTypes.get(metric);
      GenericColumnSerializer writer;
      switch (type) {
        case LONG:
          writer = LongColumnSerializer.create(ioPeon, metric, metCompression, longEncoding);
          break;
        case FLOAT:
          writer = FloatColumnSerializer.create(ioPeon, metric, metCompression);
          break;
        case DOUBLE:
          writer = DoubleColumnSerializer.create(ioPeon, metric, metCompression);
          break;
        case COMPLEX:
          final String typeName = metricTypeNames.get(metric);
          ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Unknown type[%s]", typeName);
          }
          writer = serde.getSerializer(ioPeon, metric);
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

  private void writeDimValueAndSetupDimConversion(
      final List<IndexableAdapter> indexes,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<DimensionMerger> mergers
  ) throws IOException
  {
    final String section = "setup dimension conversions";
    progress.startSection(section);

    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      mergers.get(dimIndex).writeMergedValueMetadata(indexes);
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
          mergedCapabilities.setType(null);
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

  @Override
  public File persist(
      final IncrementalIndex index,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return persist(index, index.getInterval(), outDir, indexSpec);
  }

  @Override
  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return persist(index, dataInterval, outDir, indexSpec, new BaseProgressIndicator());
  }

  @Override
  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    if (index.isEmpty()) {
      throw new IAE("Trying to persist an empty index!");
    }

    final long firstTimestamp = index.getMinTime().getMillis();
    final long lastTimestamp = index.getMaxTime().getMillis();
    if (!(dataInterval.contains(firstTimestamp) && dataInterval.contains(lastTimestamp))) {
      throw new IAE(
          "interval[%s] does not encapsulate the full range of timestamps[%s, %s]",
          dataInterval,
          new DateTime(firstTimestamp),
          new DateTime(lastTimestamp)
      );
    }

    FileUtils.forceMkdir(outDir);

    log.info("Starting persist for interval[%s], rows[%,d]", dataInterval, index.size());
    return merge(
        Arrays.<IndexableAdapter>asList(
            new IncrementalIndexAdapter(
                dataInterval,
                index,
                indexSpec.getBitmapSerdeFactory().getBitmapFactory()
            )
        ),
        // if index is not rolled up, then it should be not rollup here
        // if index is rolled up, then it is no need to rollup again.
        //                     In this case, true/false won't cause reOrdering in merge stage
        //                     while merging a single iterable
        false,
        index.getMetricAggs(),
        outDir,
        indexSpec,
        progress
    );
  }

  @Override
  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return mergeQueryableIndex(indexes, rollup, metricAggs, outDir, indexSpec, new BaseProgressIndicator());
  }

  @Override
  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    return merge(
        IndexMerger.toIndexableAdapters(indexes),
        rollup,
        metricAggs,
        outDir,
        indexSpec,
        progress
    );
  }

  @Override
  public File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    return merge(indexes, rollup, metricAggs, outDir, indexSpec, new BaseProgressIndicator());
  }

  @Override
  public File merge(
      List<IndexableAdapter> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    FileUtils.forceMkdir(outDir);

    final List<String> mergedDimensions = IndexMerger.getMergedDimensions(indexes);

    final List<String> mergedMetrics = Lists.transform(
        IndexMerger.mergeIndexed(
            Lists.newArrayList(
                FunctionalIterable
                    .create(indexes)
                    .transform(
                        new Function<IndexableAdapter, Iterable<String>>()
                        {
                          @Override
                          public Iterable<String> apply(@Nullable IndexableAdapter input)
                          {
                            return input.getMetricNames();
                          }
                        }
                    )
            )
        ),
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input;
          }
        }
    );

    final AggregatorFactory[] sortedMetricAggs = new AggregatorFactory[mergedMetrics.size()];
    for (int i = 0; i < metricAggs.length; i++) {
      AggregatorFactory metricAgg = metricAggs[i];
      int metricIndex = mergedMetrics.indexOf(metricAgg.getName());
      /*
        If metricIndex is negative, one of the metricAggs was not present in the union of metrics from the indices
        we are merging
       */
      if (metricIndex > -1) {
        sortedMetricAggs[metricIndex] = metricAgg;
      }
    }

    /*
      If there is nothing at sortedMetricAggs[i], then we did not have a metricAgg whose name matched the name
      of the ith element of mergedMetrics. I.e. There was a metric in the indices to merge that we did not ask for.
     */
    for (int i = 0; i < sortedMetricAggs.length; i++) {
      if (sortedMetricAggs[i] == null) {
        throw new IAE("Indices to merge contained metric[%s], but requested metrics did not", mergedMetrics.get(i));
      }
    }

    for (int i = 0; i < mergedMetrics.size(); i++) {
      if (!sortedMetricAggs[i].getName().equals(mergedMetrics.get(i))) {
        throw new IAE(
            "Metric mismatch, index[%d] [%s] != [%s]",
            i,
            sortedMetricAggs[i].getName(),
            mergedMetrics.get(i)
        );
      }
    }

    Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn = new Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>>()
    {
      @Override
      public Iterable<Rowboat> apply(
          @Nullable ArrayList<Iterable<Rowboat>> boats
      )
      {
        if (rollup) {
          return CombiningIterable.create(
              new MergeIterable<>(Comparators.naturalNullsFirst(), boats),
              Comparators.naturalNullsFirst(),
              new RowboatMergeFunction(sortedMetricAggs)
          );
        } else {
          return new MergeIterable<Rowboat>(
              new Ordering<Rowboat>()
              {
                @Override
                public int compare(Rowboat left, Rowboat right)
                {
                  return Longs.compare(left.getTimestamp(), right.getTimestamp());
                }
              }.nullsFirst(),
              boats
          );
        }
      }
    };

    return makeIndexFiles(
        indexes,
        sortedMetricAggs,
        outDir,
        progress,
        mergedDimensions,
        mergedMetrics,
        rowMergerFn,
        indexSpec
    );
  }

  @Override
  public File convert(final File inDir, final File outDir, final IndexSpec indexSpec) throws IOException
  {
    return convert(inDir, outDir, indexSpec, new BaseProgressIndicator());
  }

  @Override
  public File convert(final File inDir, final File outDir, final IndexSpec indexSpec, final ProgressIndicator progress)
      throws IOException
  {
    try (QueryableIndex index = indexIO.loadIndex(inDir)) {
      final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
      return makeIndexFiles(
          ImmutableList.of(adapter),
          null,
          outDir,
          progress,
          Lists.newArrayList(adapter.getDimensionNames()),
          Lists.newArrayList(adapter.getMetricNames()),
          new Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>>()
          {
            @Nullable
            @Override
            public Iterable<Rowboat> apply(ArrayList<Iterable<Rowboat>> input)
            {
              return input.get(0);
            }
          },
          indexSpec
      );
    }
  }

  @Override
  public File append(
      List<IndexableAdapter> indexes, AggregatorFactory[] aggregators, File outDir, IndexSpec indexSpec
  ) throws IOException
  {
    return append(indexes, aggregators, outDir, indexSpec, new BaseProgressIndicator());
  }

  @Override
  public File append(
      List<IndexableAdapter> indexes,
      AggregatorFactory[] aggregators,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    FileUtils.forceMkdir(outDir);

    final List<String> mergedDimensions = IndexMerger.getMergedDimensions(indexes);

    final List<String> mergedMetrics = IndexMerger.mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return Iterables.transform(
                    input.getMetricNames(),
                    new Function<String, String>()
                    {
                      @Override
                      public String apply(@Nullable String input)
                      {
                        return input;
                      }
                    }
                );
              }
            }
        )
    );

    Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn = new Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>>()
    {
      @Override
      public Iterable<Rowboat> apply(
          @Nullable final ArrayList<Iterable<Rowboat>> boats
      )
      {
        return new MergeIterable<>(Comparators.naturalNullsFirst(), boats);
      }
    };

    return makeIndexFiles(
        indexes,
        aggregators,
        outDir,
        progress,
        mergedDimensions,
        mergedMetrics,
        rowMergerFn,
        indexSpec
    );
  }

  private DimensionHandler[] makeDimensionHandlers(
      final List<String> mergedDimensions,
      final List<ColumnCapabilitiesImpl> dimCapabilities
  )
  {
    final DimensionHandler[] handlers = new DimensionHandler[mergedDimensions.size()];
    for (int i = 0; i < mergedDimensions.size(); i++) {
      ColumnCapabilities capabilities = dimCapabilities.get(i);
      String dimName = mergedDimensions.get(i);
      handlers[i] = DimensionHandlerUtils.getHandlerFromCapabilities(dimName, capabilities, null);
    }
    return handlers;
  }

  private Iterable<Rowboat> makeRowIterable(
      List<IndexableAdapter> indexes,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      Function<ArrayList<Iterable<Rowboat>>, Iterable<Rowboat>> rowMergerFn,
      final List<ColumnCapabilitiesImpl> dimCapabilities,
      final DimensionHandler[] handlers,
      final List<DimensionMerger> mergers
  )
  {
    ArrayList<Iterable<Rowboat>> boats = Lists.newArrayListWithCapacity(indexes.size());

    for (int i = 0; i < indexes.size(); ++i) {
      final IndexableAdapter adapter = indexes.get(i);

      final int[] dimLookup = getColumnIndexReorderingMap(adapter.getDimensionNames(), mergedDimensions);
      final int[] metricLookup = getColumnIndexReorderingMap(adapter.getMetricNames(), mergedMetrics);

      Iterable<Rowboat> target = indexes.get(i).getRows();
      if (dimLookup != null || metricLookup != null) {
        // resize/reorder index table if needed
        target = Iterables.transform(
            target,
            new Function<Rowboat, Rowboat>()
            {
              @Override
              public Rowboat apply(Rowboat input)
              {
                Object[] newDims;
                if (dimLookup != null) {
                  newDims = new Object[mergedDimensions.size()];
                  int j = 0;
                  for (Object dim : input.getDims()) {
                    newDims[dimLookup[j]] = dim;
                    j++;
                  }
                } else {
                  // It's possible for getColumnIndexReorderingMap to return null when
                  // both column lists are identical. Copy the old array, no dimension reordering is needed.
                  newDims = input.getDims();
                }

                Object[] newMetrics = input.getMetrics();
                if (metricLookup != null) {
                  newMetrics = new Object[mergedMetrics.size()];
                  int j = 0;
                  for (Object met : input.getMetrics()) {
                    newMetrics[metricLookup[j]] = met;
                    j++;
                  }
                }

                return new Rowboat(
                    input.getTimestamp(),
                    newDims,
                    newMetrics,
                    input.getRowNum(),
                    handlers
                );
              }
            }
        );
      }
      boats.add(
          new MMappedIndexRowIterable(
              target, mergedDimensions, i, dimCapabilities, mergers
          )
      );
    }

    return rowMergerFn.apply(boats);
  }

  // If an adapter's column list differs from the merged column list across multiple indexes,
  // return an array that maps the adapter's column orderings to the larger, merged column ordering
  private int[] getColumnIndexReorderingMap(Indexed<String> adapterColumnNames, List<String> mergedColumnNames)
  {
    if (isSame(adapterColumnNames, mergedColumnNames)) {
      return null;  // no need to convert if column lists are identical
    }
    int[] dimLookup = new int[mergedColumnNames.size()];
    for (int i = 0; i < adapterColumnNames.size(); i++) {
      dimLookup[i] = mergedColumnNames.indexOf(adapterColumnNames.get(i));
    }
    return dimLookup;
  }

  private boolean isSame(Indexed<String> indexed, List<String> values)
  {
    if (indexed.size() != values.size()) {
      return false;
    }
    for (int i = 0; i < indexed.size(); i++) {
      if (!indexed.get(i).equals(values.get(i))) {
        return false;
      }
    }
    return true;
  }

}
