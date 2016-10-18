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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.ISE;
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
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnSerializer;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.FloatGenericColumnPartSerde;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
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

      for(int i = 0; i < mergedDimensions.size(); i++) {
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
}
