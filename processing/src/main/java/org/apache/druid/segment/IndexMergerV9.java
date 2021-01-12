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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.ZeroCopyByteArrayOutputStream;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.serde.DoubleNumericColumnPartSerde;
import org.apache.druid.segment.serde.DoubleNumericColumnPartSerdeV2;
import org.apache.druid.segment.serde.FloatNumericColumnPartSerde;
import org.apache.druid.segment.serde.FloatNumericColumnPartSerdeV2;
import org.apache.druid.segment.serde.LongNumericColumnPartSerde;
import org.apache.druid.segment.serde.LongNumericColumnPartSerdeV2;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IndexMergerV9 implements IndexMerger
{
  private static final Logger log = new Logger(IndexMergerV9.class);

  // merge logic for the state capabilities will be in after incremental index is persisted
  public static final ColumnCapabilities.CoercionLogic DIMENSION_CAPABILITY_MERGE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return true;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return true;
        }

        @Override
        public boolean dictionaryValuesUnique()
        {
          return true;
        }

        @Override
        public boolean multipleValues()
        {
          return false;
        }

        @Override
        public boolean hasNulls()
        {
          return false;
        }
      };

  public static final ColumnCapabilities.CoercionLogic METRIC_CAPABILITY_MERGE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesUnique()
        {
          return false;
        }

        @Override
        public boolean multipleValues()
        {
          return false;
        }

        @Override
        public boolean hasNulls()
        {
          return false;
        }
      };

  private final ObjectMapper mapper;
  private final IndexIO indexIO;
  private final SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory;

  @Inject
  public IndexMergerV9(ObjectMapper mapper, IndexIO indexIO, SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory)
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");
    this.defaultSegmentWriteOutMediumFactory =
        Preconditions.checkNotNull(defaultSegmentWriteOutMediumFactory, "null SegmentWriteOutMediumFactory");
  }

  private File makeIndexFiles(
      final List<IndexableAdapter> adapters,
      final @Nullable AggregatorFactory[] metricAggs,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      final boolean fillRowNumConversions,
      final IndexSpec indexSpec,
      final @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    progress.start();
    progress.progress();

    List<Metadata> metadataList = Lists.transform(adapters, IndexableAdapter::getMetadata);

    final Metadata segmentMetadata;
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
      org.apache.commons.io.FileUtils.forceMkdir(outDir);

      SegmentWriteOutMediumFactory omf = segmentWriteOutMediumFactory != null ? segmentWriteOutMediumFactory
                                                                              : defaultSegmentWriteOutMediumFactory;
      log.debug("Using SegmentWriteOutMediumFactory[%s]", omf.getClass().getSimpleName());
      SegmentWriteOutMedium segmentWriteOutMedium = omf.makeSegmentWriteOutMedium(outDir);
      closer.register(segmentWriteOutMedium);
      long startTime = System.currentTimeMillis();
      Files.asByteSink(new File(outDir, "version.bin")).write(Ints.toByteArray(IndexIO.V9_VERSION));
      log.debug("Completed version.bin in %,d millis.", System.currentTimeMillis() - startTime);

      progress.progress();
      startTime = System.currentTimeMillis();
      try (FileOutputStream fos = new FileOutputStream(new File(outDir, "factory.json"))) {
        SegmentizerFactory customSegmentLoader = indexSpec.getSegmentLoader();
        if (customSegmentLoader != null) {
          mapper.writeValue(fos, customSegmentLoader);
        } else {
          mapper.writeValue(fos, new MMappedQueryableSegmentizerFactory(indexIO));
        }
      }
      log.debug("Completed factory.json in %,d millis", System.currentTimeMillis() - startTime);

      progress.progress();
      final Map<String, ValueType> metricsValueTypes = new TreeMap<>(Comparators.naturalNullsFirst());
      final Map<String, String> metricTypeNames = new TreeMap<>(Comparators.naturalNullsFirst());
      final List<ColumnCapabilities> dimCapabilities = Lists.newArrayListWithCapacity(mergedDimensions.size());
      mergeCapabilities(adapters, mergedDimensions, metricsValueTypes, metricTypeNames, dimCapabilities);

      final Map<String, DimensionHandler> handlers = makeDimensionHandlers(mergedDimensions, dimCapabilities);
      final List<DimensionMergerV9> mergers = new ArrayList<>();
      for (int i = 0; i < mergedDimensions.size(); i++) {
        DimensionHandler handler = handlers.get(mergedDimensions.get(i));
        mergers.add(handler.makeMerger(indexSpec, segmentWriteOutMedium, dimCapabilities.get(i), progress, closer));
      }

      /************* Setup Dim Conversions **************/
      progress.progress();
      startTime = System.currentTimeMillis();
      writeDimValuesAndSetupDimConversion(adapters, progress, mergedDimensions, mergers);
      log.debug("Completed dim conversions in %,d millis.", System.currentTimeMillis() - startTime);

      /************* Walk through data sets, merge them, and write merged columns *************/
      progress.progress();
      final TimeAndDimsIterator timeAndDimsIterator = makeMergedTimeAndDimsIterator(
          adapters,
          mergedDimensions,
          mergedMetrics,
          rowMergerFn,
          handlers,
          mergers
      );
      closer.register(timeAndDimsIterator);
      final GenericColumnSerializer timeWriter = setupTimeWriter(segmentWriteOutMedium, indexSpec);
      final ArrayList<GenericColumnSerializer> metricWriters =
          setupMetricsWriters(segmentWriteOutMedium, mergedMetrics, metricsValueTypes, metricTypeNames, indexSpec);
      List<IntBuffer> rowNumConversions = mergeIndexesAndWriteColumns(
          adapters,
          progress,
          timeAndDimsIterator,
          timeWriter,
          metricWriters,
          mergers,
          fillRowNumConversions
      );

      /************ Create Inverted Indexes and Finalize Build Columns *************/
      final String section = "build inverted index and columns";
      progress.startSection(section);
      makeTimeColumn(v9Smoosher, progress, timeWriter, indexSpec);
      makeMetricsColumns(
          v9Smoosher,
          progress,
          mergedMetrics,
          metricsValueTypes,
          metricTypeNames,
          metricWriters,
          indexSpec
      );

      for (int i = 0; i < mergedDimensions.size(); i++) {
        DimensionMergerV9 merger = mergers.get(i);
        merger.writeIndexes(rowNumConversions);
        if (merger.canSkip()) {
          continue;
        }
        ColumnDescriptor columnDesc = merger.makeColumnDescriptor();
        makeColumn(v9Smoosher, mergedDimensions.get(i), columnDesc);
      }

      progress.stopSection(section);

      /************* Make index.drd & metadata.drd files **************/
      progress.progress();
      makeIndexBinary(v9Smoosher, adapters, outDir, mergedDimensions, mergedMetrics, progress, indexSpec, mergers);
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
      final List<DimensionMergerV9> mergers
  ) throws IOException
  {
    final String section = "make index.drd";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final Set<String> finalDimensions = new LinkedHashSet<>();
    final Set<String> finalColumns = new LinkedHashSet<>(mergedMetrics);
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
                          + SERIALIZER_UTILS.getSerializedStringByteSize(bitmapSerdeFactoryType);

    final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);
    cols.writeTo(writer, v9Smoosher);
    dims.writeTo(writer, v9Smoosher);

    DateTime minTime = DateTimes.MAX;
    DateTime maxTime = DateTimes.MIN;

    for (IndexableAdapter index : adapters) {
      minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
      maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
    }
    final Interval dataInterval = new Interval(minTime, maxTime);

    SERIALIZER_UTILS.writeLong(writer, dataInterval.getStartMillis());
    SERIALIZER_UTILS.writeLong(writer, dataInterval.getEndMillis());

    SERIALIZER_UTILS.writeString(writer, bitmapSerdeFactoryType);
    writer.close();

    IndexIO.checkFileSize(new File(outDir, "index.drd"));
    log.debug("Completed index.drd in %,d millis.", System.currentTimeMillis() - startTime);

    progress.stopSection(section);
  }

  private void makeMetricsColumns(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final List<String> mergedMetrics,
      final Map<String, ValueType> metricsValueTypes,
      final Map<String, String> metricTypeNames,
      final List<GenericColumnSerializer> metWriters,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "make metric columns";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < mergedMetrics.size(); ++i) {
      String metric = mergedMetrics.get(i);
      long metricStartTime = System.currentTimeMillis();
      GenericColumnSerializer writer = metWriters.get(i);

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
      ValueType type = metricsValueTypes.get(metric);
      switch (type) {
        case LONG:
          builder.setValueType(ValueType.LONG);
          builder.addSerde(createLongColumnPartSerde(writer, indexSpec));
          break;
        case FLOAT:
          builder.setValueType(ValueType.FLOAT);
          builder.addSerde(createFloatColumnPartSerde(writer, indexSpec));
          break;
        case DOUBLE:
          builder.setValueType(ValueType.DOUBLE);
          builder.addSerde(createDoubleColumnPartSerde(writer, indexSpec));
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
      log.debug("Completed metric column[%s] in %,d millis.", metric, System.currentTimeMillis() - metricStartTime);
    }
    log.debug("Completed metric columns in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  static ColumnPartSerde createLongColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    // If using default values for null use LongNumericColumnPartSerde to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return LongNumericColumnPartSerde.serializerBuilder()
                                       .withByteOrder(IndexIO.BYTE_ORDER)
                                       .withDelegate(serializer)
                                       .build();
    } else {
      return LongNumericColumnPartSerdeV2.serializerBuilder()
                                         .withByteOrder(IndexIO.BYTE_ORDER)
                                         .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                         .withDelegate(serializer)
                                         .build();
    }
  }

  static ColumnPartSerde createDoubleColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    // If using default values for null use DoubleNumericColumnPartSerde to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return DoubleNumericColumnPartSerde.serializerBuilder()
                                         .withByteOrder(IndexIO.BYTE_ORDER)
                                         .withDelegate(serializer)
                                         .build();
    } else {
      return DoubleNumericColumnPartSerdeV2.serializerBuilder()
                                           .withByteOrder(IndexIO.BYTE_ORDER)
                                           .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                           .withDelegate(serializer)
                                           .build();
    }
  }

  static ColumnPartSerde createFloatColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    // If using default values for null use FloatNumericColumnPartSerde to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return FloatNumericColumnPartSerde.serializerBuilder()
                                        .withByteOrder(IndexIO.BYTE_ORDER)
                                        .withDelegate(serializer)
                                        .build();
    } else {
      return FloatNumericColumnPartSerdeV2.serializerBuilder()
                                          .withByteOrder(IndexIO.BYTE_ORDER)
                                          .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                          .withDelegate(serializer)
                                          .build();
    }
  }

  private void makeTimeColumn(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final GenericColumnSerializer timeWriter,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "make time column";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    final ColumnDescriptor serdeficator = ColumnDescriptor
        .builder()
        .setValueType(ValueType.LONG)
        .addSerde(createLongColumnPartSerde(timeWriter, indexSpec))
        .build();
    makeColumn(v9Smoosher, ColumnHolder.TIME_COLUMN_NAME, serdeficator);
    log.debug("Completed time column in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  private void makeColumn(
      final FileSmoosher v9Smoosher,
      final String columnName,
      final ColumnDescriptor serdeficator
  ) throws IOException
  {
    ZeroCopyByteArrayOutputStream specBytes = new ZeroCopyByteArrayOutputStream();
    SERIALIZER_UTILS.writeString(specBytes, mapper.writeValueAsString(serdeficator));
    try (SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
        columnName,
        specBytes.size() + serdeficator.getSerializedSize()
    )) {
      specBytes.writeTo(channel);
      serdeficator.writeTo(channel, v9Smoosher);
    }
  }

  /**
   * Returns rowNumConversions, if fillRowNumConversions argument is true
   */
  @Nullable
  private List<IntBuffer> mergeIndexesAndWriteColumns(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final TimeAndDimsIterator timeAndDimsIterator,
      final GenericColumnSerializer timeWriter,
      final ArrayList<GenericColumnSerializer> metricWriters,
      final List<DimensionMergerV9> mergers,
      final boolean fillRowNumConversions
  ) throws IOException
  {
    final String section = "walk through and merge rows";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    List<IntBuffer> rowNumConversions = null;
    int rowCount = 0;
    if (fillRowNumConversions) {
      rowNumConversions = new ArrayList<>(adapters.size());
      for (IndexableAdapter adapter : adapters) {
        int[] arr = new int[adapter.getNumRows()];
        Arrays.fill(arr, INVALID_ROW);
        rowNumConversions.add(IntBuffer.wrap(arr));
      }
    }

    long time = System.currentTimeMillis();
    while (timeAndDimsIterator.moveToNext()) {
      progress.progress();
      TimeAndDimsPointer timeAndDims = timeAndDimsIterator.getPointer();
      timeWriter.serialize(timeAndDims.timestampSelector);

      for (int metricIndex = 0; metricIndex < timeAndDims.getNumMetrics(); metricIndex++) {
        metricWriters.get(metricIndex).serialize(timeAndDims.getMetricSelector(metricIndex));
      }

      for (int dimIndex = 0; dimIndex < timeAndDims.getNumDimensions(); dimIndex++) {
        DimensionMerger merger = mergers.get(dimIndex);
        if (merger.canSkip()) {
          continue;
        }
        merger.processMergedRow(timeAndDims.getDimensionSelector(dimIndex));
      }

      if (timeAndDimsIterator instanceof RowCombiningTimeAndDimsIterator) {
        RowCombiningTimeAndDimsIterator comprisedRows = (RowCombiningTimeAndDimsIterator) timeAndDimsIterator;

        for (int originalIteratorIndex = comprisedRows.nextCurrentlyCombinedOriginalIteratorIndex(0);
             originalIteratorIndex >= 0;
             originalIteratorIndex =
                 comprisedRows.nextCurrentlyCombinedOriginalIteratorIndex(originalIteratorIndex + 1)) {

          IntBuffer conversionBuffer = rowNumConversions.get(originalIteratorIndex);
          int minRowNum = comprisedRows.getMinCurrentlyCombinedRowNumByOriginalIteratorIndex(originalIteratorIndex);
          int maxRowNum = comprisedRows.getMaxCurrentlyCombinedRowNumByOriginalIteratorIndex(originalIteratorIndex);

          for (int rowNum = minRowNum; rowNum <= maxRowNum; rowNum++) {
            while (conversionBuffer.position() < rowNum) {
              conversionBuffer.put(INVALID_ROW);
            }
            conversionBuffer.put(rowCount);
          }

        }

      } else if (timeAndDimsIterator instanceof MergingRowIterator) {
        RowPointer rowPointer = (RowPointer) timeAndDims;
        IntBuffer conversionBuffer = rowNumConversions.get(rowPointer.getIndexNum());
        int rowNum = rowPointer.getRowNum();
        while (conversionBuffer.position() < rowNum) {
          conversionBuffer.put(INVALID_ROW);
        }
        conversionBuffer.put(rowCount);
      } else {
        if (fillRowNumConversions) {
          throw new IllegalStateException(
              "Filling row num conversions is supported only with RowCombining and Merging iterators"
          );
        }
      }

      if ((++rowCount % 500000) == 0) {
        log.debug("walked 500,000/%d rows in %,d millis.", rowCount, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
      }
    }
    if (rowNumConversions != null) {
      for (IntBuffer rowNumConversion : rowNumConversions) {
        rowNumConversion.rewind();
      }
    }
    log.debug("completed walk through of %,d rows in %,d millis.", rowCount, System.currentTimeMillis() - startTime);
    progress.stopSection(section);
    return rowNumConversions;
  }

  private GenericColumnSerializer setupTimeWriter(SegmentWriteOutMedium segmentWriteOutMedium, IndexSpec indexSpec)
      throws IOException
  {
    GenericColumnSerializer timeWriter = createLongColumnSerializer(
        segmentWriteOutMedium,
        "little_end_time",
        indexSpec
    );
    // we will close this writer after we added all the timestamps
    timeWriter.open();
    return timeWriter;
  }

  private ArrayList<GenericColumnSerializer> setupMetricsWriters(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final List<String> mergedMetrics,
      final Map<String, ValueType> metricsValueTypes,
      final Map<String, String> metricTypeNames,
      final IndexSpec indexSpec
  ) throws IOException
  {
    ArrayList<GenericColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());

    for (String metric : mergedMetrics) {
      ValueType type = metricsValueTypes.get(metric);
      GenericColumnSerializer writer;
      switch (type) {
        case LONG:
          writer = createLongColumnSerializer(segmentWriteOutMedium, metric, indexSpec);
          break;
        case FLOAT:
          writer = createFloatColumnSerializer(segmentWriteOutMedium, metric, indexSpec);
          break;
        case DOUBLE:
          writer = createDoubleColumnSerializer(segmentWriteOutMedium, metric, indexSpec);
          break;
        case COMPLEX:
          final String typeName = metricTypeNames.get(metric);
          ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Unknown type[%s]", typeName);
          }
          writer = serde.getSerializer(segmentWriteOutMedium, metric);
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

  static GenericColumnSerializer createLongColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      IndexSpec indexSpec
  )
  {
    // If using default values for null use LongColumnSerializer to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return LongColumnSerializer.create(
          columnName,
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression(),
          indexSpec.getLongEncoding()
      );
    } else {
      return LongColumnSerializerV2.create(
          columnName,
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression(),
          indexSpec.getLongEncoding(),
          indexSpec.getBitmapSerdeFactory()
      );
    }
  }

  static GenericColumnSerializer createDoubleColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      IndexSpec indexSpec
  )
  {
    // If using default values for null use DoubleColumnSerializer to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return DoubleColumnSerializer.create(
          columnName,
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression()
      );
    } else {
      return DoubleColumnSerializerV2.create(
          columnName,
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression(),
          indexSpec.getBitmapSerdeFactory()
      );
    }
  }

  static GenericColumnSerializer createFloatColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      IndexSpec indexSpec
  )
  {
    // If using default values for null use FloatColumnSerializer to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return FloatColumnSerializer.create(
          columnName,
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression()
      );
    } else {
      return FloatColumnSerializerV2.create(
          columnName,
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression(),
          indexSpec.getBitmapSerdeFactory()
      );
    }
  }

  private void writeDimValuesAndSetupDimConversion(
      final List<IndexableAdapter> indexes,
      final ProgressIndicator progress,
      final List<String> mergedDimensions,
      final List<DimensionMergerV9> mergers
  ) throws IOException
  {
    final String section = "setup dimension conversions";
    progress.startSection(section);

    for (int dimIndex = 0; dimIndex < mergedDimensions.size(); ++dimIndex) {
      mergers.get(dimIndex).writeMergedValueDictionary(indexes);
    }
    progress.stopSection(section);
  }

  private void mergeCapabilities(
      final List<IndexableAdapter> adapters,
      final List<String> mergedDimensions,
      final Map<String, ValueType> metricsValueTypes,
      final Map<String, String> metricTypeNames,
      final List<ColumnCapabilities> dimCapabilities
  )
  {
    final Map<String, ColumnCapabilities> capabilitiesMap = new HashMap<>();
    for (IndexableAdapter adapter : adapters) {
      for (String dimension : adapter.getDimensionNames()) {
        ColumnCapabilities capabilities = adapter.getCapabilities(dimension);
        capabilitiesMap.compute(dimension, (d, existingCapabilities) ->
            ColumnCapabilitiesImpl.merge(capabilities, existingCapabilities, DIMENSION_CAPABILITY_MERGE_LOGIC)
        );
      }
      for (String metric : adapter.getMetricNames()) {
        ColumnCapabilities capabilities = adapter.getCapabilities(metric);
        capabilitiesMap.compute(metric, (m, existingCapabilities) ->
            ColumnCapabilitiesImpl.merge(capabilities, existingCapabilities, METRIC_CAPABILITY_MERGE_LOGIC)
        );
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
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return persist(index, index.getInterval(), outDir, indexSpec, segmentWriteOutMediumFactory);
  }

  @Override
  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return persist(index, dataInterval, outDir, indexSpec, new BaseProgressIndicator(), segmentWriteOutMediumFactory);
  }

  @Override
  public File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    if (index.isEmpty()) {
      throw new IAE("Trying to persist an empty index!");
    }

    final DateTime firstTimestamp = index.getMinTime();
    final DateTime lastTimestamp = index.getMaxTime();
    if (!(dataInterval.contains(firstTimestamp) && dataInterval.contains(lastTimestamp))) {
      throw new IAE(
          "interval[%s] does not encapsulate the full range of timestamps[%s, %s]",
          dataInterval,
          firstTimestamp,
          lastTimestamp
      );
    }

    org.apache.commons.io.FileUtils.forceMkdir(outDir);

    log.debug("Starting persist for interval[%s], rows[%,d]", dataInterval, index.size());
    return multiphaseMerge(
        Collections.singletonList(
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
        progress,
        segmentWriteOutMediumFactory,
        -1
    );
  }

  @Override
  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException
  {
    return mergeQueryableIndex(
        indexes,
        rollup,
        metricAggs,
        outDir,
        indexSpec,
        new BaseProgressIndicator(),
        segmentWriteOutMediumFactory,
        maxColumnsToMerge
    );
  }

  @Override
  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException
  {
    return multiphaseMerge(
        IndexMerger.toIndexableAdapters(indexes),
        rollup,
        metricAggs,
        outDir,
        indexSpec,
        progress,
        segmentWriteOutMediumFactory,
        maxColumnsToMerge
    );
  }

  @Override
  public File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      int maxColumnsToMerge
  ) throws IOException
  {
    return multiphaseMerge(indexes, rollup, metricAggs, outDir, indexSpec, new BaseProgressIndicator(), null, maxColumnsToMerge);
  }

  private File multiphaseMerge(
      List<IndexableAdapter> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    org.apache.commons.io.FileUtils.forceMkdir(outDir);

    List<File> tempDirs = new ArrayList<>();

    if (maxColumnsToMerge == IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE) {
      return merge(
          indexes,
          rollup,
          metricAggs,
          outDir,
          indexSpec,
          progress,
          segmentWriteOutMediumFactory
      );
    }

    List<List<IndexableAdapter>> currentPhases = getMergePhases(indexes, maxColumnsToMerge);
    List<File> currentOutputs = new ArrayList<>();

    log.debug("base outDir: " + outDir);

    try {
      int tierCounter = 0;
      while (true) {
        log.info("Merging %d phases, tiers finished processed so far: %d.", currentPhases.size(), tierCounter);
        for (List<IndexableAdapter> phase : currentPhases) {
          File phaseOutDir;
          if (currentPhases.size() == 1) {
            // use the given outDir on the final merge phase
            phaseOutDir = outDir;
            log.info("Performing final merge phase.");
          } else {
            phaseOutDir = FileUtils.createTempDir();
            tempDirs.add(phaseOutDir);
          }
          log.info("Merging phase with %d indexes.", phase.size());
          log.debug("phase outDir: " + phaseOutDir);

          File phaseOutput = merge(
              phase,
              rollup,
              metricAggs,
              phaseOutDir,
              indexSpec,
              progress,
              segmentWriteOutMediumFactory
          );
          currentOutputs.add(phaseOutput);
        }
        if (currentOutputs.size() == 1) {
          // we're done, we made a single File output
          return currentOutputs.get(0);
        } else {
          // convert Files to QueryableIndexIndexableAdapter and do another merge phase
          List<IndexableAdapter> qIndexAdapters = new ArrayList<>();
          for (File outputFile : currentOutputs) {
            QueryableIndex qIndex = indexIO.loadIndex(outputFile, true, SegmentLazyLoadFailCallback.NOOP);
            qIndexAdapters.add(new QueryableIndexIndexableAdapter(qIndex));
          }
          currentPhases = getMergePhases(qIndexAdapters, maxColumnsToMerge);
          currentOutputs = new ArrayList<>();
          tierCounter += 1;
        }
      }
    }
    finally {
      for (File tempDir : tempDirs) {
        if (tempDir.exists()) {
          try {
            FileUtils.deleteDirectory(tempDir);
          }
          catch (Exception e) {
            log.warn(e, "Failed to remove directory[%s]", tempDir);
          }
        }
      }
    }
  }

  private List<List<IndexableAdapter>> getMergePhases(List<IndexableAdapter> indexes, int maxColumnsToMerge)
  {
    List<List<IndexableAdapter>> toMerge = new ArrayList<>();
    // always merge at least two segments regardless of column limit
    if (indexes.size() <= 2) {
      if (getIndexColumnCount(indexes) > maxColumnsToMerge) {
        log.info("index pair has more columns than maxColumnsToMerge [%d].", maxColumnsToMerge);
      }
      toMerge.add(indexes);
    } else {
      List<IndexableAdapter> currentPhase = new ArrayList<>();
      int currentColumnCount = 0;
      for (IndexableAdapter index : indexes) {
        int indexColumnCount = getIndexColumnCount(index);
        if (indexColumnCount > maxColumnsToMerge) {
          log.info("index has more columns [%d] than maxColumnsToMerge [%d]!", indexColumnCount, maxColumnsToMerge);
        }

        // always merge at least two segments regardless of column limit
        if (currentPhase.size() > 1 && currentColumnCount + indexColumnCount > maxColumnsToMerge) {
          toMerge.add(currentPhase);
          currentPhase = new ArrayList<>();
          currentColumnCount = indexColumnCount;
          currentPhase.add(index);
        } else {
          currentPhase.add(index);
          currentColumnCount += indexColumnCount;
        }
      }
      toMerge.add(currentPhase);
    }
    return toMerge;
  }

  private int getIndexColumnCount(IndexableAdapter indexableAdapter)
  {
    // +1 for the __time column
    return 1 + indexableAdapter.getDimensionNames().size() + indexableAdapter.getMetricNames().size();
  }

  private int getIndexColumnCount(List<IndexableAdapter> indexableAdapters)
  {
    int count = 0;
    for (IndexableAdapter indexableAdapter : indexableAdapters) {
      count += getIndexColumnCount(indexableAdapter);
    }
    return count;
  }

  private File merge(
      List<IndexableAdapter> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    final List<String> mergedDimensions = IndexMerger.getMergedDimensions(indexes);

    final List<String> mergedMetrics = IndexMerger.mergeIndexed(
        indexes.stream().map(IndexableAdapter::getMetricNames).collect(Collectors.toList())
    );

    final AggregatorFactory[] sortedMetricAggs = new AggregatorFactory[mergedMetrics.size()];
    for (AggregatorFactory metricAgg : metricAggs) {
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

    Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn;
    if (rollup) {
      rowMergerFn = rowIterators -> new RowCombiningTimeAndDimsIterator(rowIterators, sortedMetricAggs, mergedMetrics);
    } else {
      rowMergerFn = MergingRowIterator::new;
    }

    return makeIndexFiles(
        indexes,
        sortedMetricAggs,
        outDir,
        progress,
        mergedDimensions,
        mergedMetrics,
        rowMergerFn,
        true,
        indexSpec,
        segmentWriteOutMediumFactory
    );
  }

  @Override
  public File convert(final File inDir, final File outDir, final IndexSpec indexSpec) throws IOException
  {
    return convert(inDir, outDir, indexSpec, new BaseProgressIndicator(), defaultSegmentWriteOutMediumFactory);
  }

  private File convert(
      final File inDir,
      final File outDir,
      final IndexSpec indexSpec,
      final ProgressIndicator progress,
      final @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
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
          Iterables::getOnlyElement,
          false,
          indexSpec,
          segmentWriteOutMediumFactory
      );
    }
  }

  @Override
  public File append(
      List<IndexableAdapter> indexes,
      AggregatorFactory[] aggregators,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    org.apache.commons.io.FileUtils.forceMkdir(outDir);

    final List<String> mergedDimensions = IndexMerger.getMergedDimensions(indexes);

    final List<String> mergedMetrics = IndexMerger.mergeIndexed(
        indexes.stream().map(IndexableAdapter::getMetricNames).collect(Collectors.toList())
    );

    return makeIndexFiles(
        indexes,
        aggregators,
        outDir,
        new BaseProgressIndicator(),
        mergedDimensions,
        mergedMetrics,
        MergingRowIterator::new,
        true,
        indexSpec,
        segmentWriteOutMediumFactory
    );
  }

  private Map<String, DimensionHandler> makeDimensionHandlers(
      final List<String> mergedDimensions,
      final List<ColumnCapabilities> dimCapabilities
  )
  {
    Map<String, DimensionHandler> handlers = new LinkedHashMap<>();
    for (int i = 0; i < mergedDimensions.size(); i++) {
      ColumnCapabilities capabilities = ColumnCapabilitiesImpl.snapshot(
          dimCapabilities.get(i),
          DIMENSION_CAPABILITY_MERGE_LOGIC
      );
      String dimName = mergedDimensions.get(i);
      DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dimName, capabilities, null);
      handlers.put(dimName, handler);
    }
    return handlers;
  }

  private TimeAndDimsIterator makeMergedTimeAndDimsIterator(
      final List<IndexableAdapter> indexes,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics,
      final Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      final Map<String, DimensionHandler> handlers,
      final List<DimensionMergerV9> mergers
  )
  {
    List<TransformableRowIterator> perIndexRowIterators = Lists.newArrayListWithCapacity(indexes.size());
    for (int i = 0; i < indexes.size(); ++i) {
      final IndexableAdapter adapter = indexes.get(i);
      TransformableRowIterator target = adapter.getRows();
      if (!mergedDimensions.equals(adapter.getDimensionNames()) || !mergedMetrics.equals(adapter.getMetricNames())) {
        target = makeRowIteratorWithReorderedColumns(
            mergedDimensions,
            mergedMetrics,
            handlers,
            adapter,
            target
        );
      }
      perIndexRowIterators.add(IndexMerger.toMergedIndexRowIterator(target, i, mergers));
    }
    return rowMergerFn.apply(perIndexRowIterators);
  }

  private TransformableRowIterator makeRowIteratorWithReorderedColumns(
      List<String> reorderedDimensions,
      List<String> reorderedMetrics,
      Map<String, DimensionHandler> originalHandlers,
      IndexableAdapter originalAdapter,
      TransformableRowIterator originalIterator
  )
  {
    RowPointer reorderedRowPointer = reorderRowPointerColumns(
        reorderedDimensions,
        reorderedMetrics,
        originalHandlers,
        originalAdapter,
        originalIterator.getPointer()
    );
    TimeAndDimsPointer reorderedMarkedRowPointer = reorderRowPointerColumns(
        reorderedDimensions,
        reorderedMetrics,
        originalHandlers,
        originalAdapter,
        originalIterator.getMarkedPointer()
    );
    return new ForwardingRowIterator(originalIterator)
    {
      @Override
      public RowPointer getPointer()
      {
        return reorderedRowPointer;
      }

      @Override
      public TimeAndDimsPointer getMarkedPointer()
      {
        return reorderedMarkedRowPointer;
      }
    };
  }

  private static <T extends TimeAndDimsPointer> T reorderRowPointerColumns(
      List<String> reorderedDimensions,
      List<String> reorderedMetrics,
      Map<String, DimensionHandler> originalHandlers,
      IndexableAdapter originalAdapter,
      T originalRowPointer
  )
  {
    ColumnValueSelector[] reorderedDimensionSelectors = reorderedDimensions
        .stream()
        .map(dimName -> {
          int dimIndex = originalAdapter.getDimensionNames().indexOf(dimName);
          if (dimIndex >= 0) {
            return originalRowPointer.getDimensionSelector(dimIndex);
          } else {
            return NilColumnValueSelector.instance();
          }
        })
        .toArray(ColumnValueSelector[]::new);
    List<DimensionHandler> reorderedHandlers =
        reorderedDimensions.stream().map(originalHandlers::get).collect(Collectors.toList());
    ColumnValueSelector[] reorderedMetricSelectors = reorderedMetrics
        .stream()
        .map(metricName -> {
          int metricIndex = originalAdapter.getMetricNames().indexOf(metricName);
          if (metricIndex >= 0) {
            return originalRowPointer.getMetricSelector(metricIndex);
          } else {
            return NilColumnValueSelector.instance();
          }
        })
        .toArray(ColumnValueSelector[]::new);
    if (originalRowPointer instanceof RowPointer) {
      //noinspection unchecked
      return (T) new RowPointer(
          originalRowPointer.timestampSelector,
          reorderedDimensionSelectors,
          reorderedHandlers,
          reorderedMetricSelectors,
          reorderedMetrics,
          ((RowPointer) originalRowPointer).rowNumPointer
      );
    } else {
      //noinspection unchecked
      return (T) new TimeAndDimsPointer(
          originalRowPointer.timestampSelector,
          reorderedDimensionSelectors,
          reorderedHandlers,
          reorderedMetricSelectors,
          reorderedMetrics
      );
    }
  }
}
