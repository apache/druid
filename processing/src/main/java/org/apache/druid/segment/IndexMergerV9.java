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
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.ZeroCopyByteArrayOutputStream;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.Column;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.serde.DoubleGenericColumnPartSerde;
import org.apache.druid.segment.serde.DoubleGenericColumnPartSerdeV2;
import org.apache.druid.segment.serde.FloatGenericColumnPartSerde;
import org.apache.druid.segment.serde.FloatGenericColumnPartSerdeV2;
import org.apache.druid.segment.serde.LongGenericColumnPartSerde;
import org.apache.druid.segment.serde.LongGenericColumnPartSerdeV2;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IndexMergerV9 implements IndexMerger
{
  private static final Logger log = new Logger(IndexMergerV9.class);

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
      final File outDir,
      final ProgressIndicator progress,
      final List<DimensionDesc> mergedDimensions,
      final List<MetricDesc> mergedMetrics,
      final Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      final boolean fillRowNumConversions,
      final IndexSpec indexSpec,
      final int mergeDegree,
      @Nullable final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    progress.start();
    progress.progress();

    List<Metadata> metadataList = Lists.transform(adapters, IndexableAdapter::getMetadata);

    final Metadata segmentMetadata;
    if (mergedMetrics.stream().allMatch(metricDesc -> metricDesc.getAggregatorFactory() != null)) {
      // mergedMetrics is empty or all of them have aggregatorFactory
      final AggregatorFactory[] combiningMetricAggs = mergedMetrics
          .stream()
          .map(metricDesc -> metricDesc.getAggregatorFactory().getCombiningFactory())
          .toArray(AggregatorFactory[]::new);
      segmentMetadata = Metadata.merge(
          metadataList,
          combiningMetricAggs
      );
    } else {
      // all mergedMetrics shouln't have aggregatorFactory
      if (mergedMetrics.stream().anyMatch(metricDesc -> metricDesc.getAggregatorFactory() != null)) {
        throw new ISE("Some metrics[%s] don't have aggregatorFactory.", mergedMetrics);
      }
      segmentMetadata = Metadata.merge(
          metadataList,
          null
      );
    }

    progress.progress();

    final File baseOutDir = outDir.getParentFile();
    final MergeOutDirSupplier outDirSupplier = new MergeOutDirSupplier(baseOutDir);
    final List<File> merged = incrementalMerge(
        adapters,
        outDirSupplier,
        progress,
        mergedDimensions,
        mergedMetrics,
        rowMergerFn,
        fillRowNumConversions,
        indexSpec,
        segmentMetadata,
        mergeDegree,
        segmentWriteOutMediumFactory
    );

    if (merged.size() == 1) {
      if (outDir.exists()) {
        final String[] filesInOutDir = outDir.list();
        if (filesInOutDir != null && filesInOutDir.length > 0) {
          log.warn("Found files[%s] in destination directory. Will delete them", Arrays.asList(filesInOutDir));
        }
        FileUtils.forceDelete(outDir);
      }

      FileUtils.moveDirectory(outDirSupplier.getLast(), outDir);

      progress.stop();

      return outDir;
    } else {
      throw new ISE("WTH? We got two or more indexes[%s] after merging?", merged);
    }
  }

  private List<File> incrementalMerge(
      List<IndexableAdapter> indexes,
      MergeOutDirSupplier outDirSupplier,
      ProgressIndicator progress,
      List<DimensionDesc> mergedDimensions,
      List<MetricDesc> mergedMetrics,
      Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      boolean fillRowNumConversions,
      IndexSpec indexSpec,
      @Nullable Metadata segmentMetadata,
      int mergeDegree,
      @Nullable final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    // Merge the input files
    final List<File> mergedDirs = new ArrayList<>();
    for (int start = 0; start < indexes.size(); start += mergeDegree) {
      final int end = Math.min(indexes.size(), start + mergeDegree);
      final List<IndexableAdapter> indexesToMerge = indexes.subList(start, end);
      final File tmpOutDir = outDirSupplier.get();
      mergeAndWriteFiles(
          indexesToMerge,
          tmpOutDir,
          progress,
          mergedDimensions,
          mergedMetrics,
          rowMergerFn,
          fillRowNumConversions,
          indexSpec,
          segmentMetadata,
          segmentWriteOutMediumFactory
      );
      mergedDirs.add(tmpOutDir);
    }
    // Merge recursively
    return recursiveMerge(
        mergedDirs,
        outDirSupplier,
        progress,
        mergedDimensions,
        mergedMetrics,
        rowMergerFn,
        fillRowNumConversions,
        indexSpec,
        segmentMetadata,
        mergeDegree,
        segmentWriteOutMediumFactory
    );
  }

  private List<File> recursiveMerge(
      List<File> indexDirs,
      MergeOutDirSupplier outDirSupplier,
      ProgressIndicator progress,
      List<DimensionDesc> mergedDimensions,
      List<MetricDesc> mergedMetrics,
      Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      boolean fillRowNumConversions,
      IndexSpec indexSpec,
      @Nullable Metadata segmentMetadata,
      int mergeDegree,
      @Nullable final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    if (indexDirs.size() < 2) {
      return indexDirs;
    } else {
      final List<File> mergedDirs = new ArrayList<>();

      for (int start = 0; start < indexDirs.size(); start += mergeDegree) {
        final Closer closer = Closer.create();
        final int end = Math.min(indexDirs.size(), start + mergeDegree);
        final List<File> dirsToMerge = indexDirs.subList(start, end);
        final List<IndexableAdapter> indexesToMerge = dirsToMerge.stream().map(dir -> {
          try {
            final QueryableIndexAdapter adapter = new QueryableIndexAdapter(indexIO.loadIndex(dir));
            // Closeables are stored in a stack in closer.
            // The below is to close adapter first and then delete the file.
            closer.register(() -> FileUtils.forceDelete(dir));
            closer.register(adapter);
            return adapter;
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());

        final File tmpOutDir = outDirSupplier.get();
        mergeAndWriteFiles(
            indexesToMerge,
            tmpOutDir,
            progress,
            mergedDimensions,
            mergedMetrics,
            rowMergerFn,
            fillRowNumConversions,
            indexSpec,
            segmentMetadata,
            segmentWriteOutMediumFactory
        );
        mergedDirs.add(tmpOutDir);
        closer.close();
      }

      return recursiveMerge(
          mergedDirs,
          outDirSupplier,
          progress,
          mergedDimensions,
          mergedMetrics,
          rowMergerFn,
          fillRowNumConversions,
          indexSpec,
          segmentMetadata,
          mergeDegree,
          segmentWriteOutMediumFactory
      );
    }
  }

  private void mergeAndWriteFiles(
      List<IndexableAdapter> adapters,
      File outDir,
      ProgressIndicator progress,
      List<DimensionDesc> mergedDimensions,
      List<MetricDesc> mergedMetrics,
      Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      boolean fillRowNumConversions,
      IndexSpec indexSpec,
      @Nullable Metadata segmentMetadata,
      @Nullable final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    final Closer closer = Closer.create();

    try {
      final FileSmoosher v9Smoosher = new FileSmoosher(outDir);
      FileUtils.forceMkdir(outDir);

      long startTime = System.currentTimeMillis();
      Files.asByteSink(new File(outDir, "version.bin")).write(Ints.toByteArray(IndexIO.V9_VERSION));
      log.info("Completed version.bin in %,d millis.", System.currentTimeMillis() - startTime);

      progress.progress();
      startTime = System.currentTimeMillis();
      try (FileOutputStream fos = new FileOutputStream(new File(outDir, "factory.json"))) {
        mapper.writeValue(fos, new MMappedQueryableSegmentizerFactory(indexIO));
      }
      log.info("Completed factory.json in %,d millis", System.currentTimeMillis() - startTime);

      SegmentWriteOutMediumFactory omf = segmentWriteOutMediumFactory != null ? segmentWriteOutMediumFactory
                                                                              : defaultSegmentWriteOutMediumFactory;
      log.info("Using SegmentWriteOutMediumFactory[%s]", omf.getClass().getSimpleName());
      SegmentWriteOutMedium segmentWriteOutMedium = omf.makeSegmentWriteOutMedium(outDir);
      closer.register(segmentWriteOutMedium);

      mergedDimensions.forEach(dimensionDesc -> dimensionDesc.makeMerger(indexSpec, segmentWriteOutMedium, progress));

      /************* Setup Dim Conversions **************/
      progress.progress();
      startTime = System.currentTimeMillis();
      writeDimValuesAndSetupDimConversion(adapters, progress, mergedDimensions);
      log.info("Completed dim conversions in %,d millis.", System.currentTimeMillis() - startTime);

      /************* Walk through data sets, merge them, and write merged columns *************/
      progress.progress();

      final TimeAndDimsIterator timeAndDimsIterator = makeMergedTimeAndDimsIterator(
          adapters,
          mergedDimensions,
          mergedMetrics,
          rowMergerFn
      );
      closer.register(timeAndDimsIterator);
      final GenericColumnSerializer timeWriter = setupTimeWriter(segmentWriteOutMedium, indexSpec);
      final ArrayList<GenericColumnSerializer> metricWriters =
          setupMetricsWriters(segmentWriteOutMedium, mergedMetrics, indexSpec);
      List<IntBuffer> rowNumConversions = mergeIndexesAndWriteColumns(
          adapters,
          progress,
          timeAndDimsIterator,
          timeWriter,
          metricWriters,
          mergedDimensions,
          fillRowNumConversions
      );

      /************ Create Inverted Indexes and Finalize Build Columns *************/
      makeTimeColumn(v9Smoosher, progress, timeWriter, indexSpec);
      makeMetricsColumns(
          v9Smoosher,
          progress,
          mergedMetrics,
          metricWriters,
          indexSpec
      );

      for (DimensionDesc mergedDimension : mergedDimensions) {
        DimensionMergerV9 merger = (DimensionMergerV9) mergedDimension.getNonNullMerger();
        merger.writeIndexes(rowNumConversions);
        if (merger.canSkip()) {
          continue;
        }
        ColumnDescriptor columnDesc = merger.makeColumnDescriptor();
        makeColumn(v9Smoosher, mergedDimension.getName(), columnDesc);
      }


      /************* Make index.drd & metadata.drd files **************/
      progress.progress();
      makeIndexBinary(v9Smoosher, adapters, outDir, mergedDimensions, mergedMetrics, progress, indexSpec);
      makeMetadataBinary(v9Smoosher, progress, segmentMetadata);

      v9Smoosher.close();
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
      @Nullable final Metadata segmentMetadata
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
      final List<DimensionDesc> mergedDimensions,
      final List<MetricDesc> mergedMetrics,
      final ProgressIndicator progress,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "make index.drd";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();
    final Set<String> finalDimensions = Sets.newLinkedHashSet();
    final Set<String> finalColumns = Sets.newLinkedHashSet();
    finalColumns.addAll(mergedMetrics.stream().map(MetricDesc::getName).collect(Collectors.toList()));
    for (DimensionDesc mergedDimension : mergedDimensions) {
      if (mergedDimension.getNonNullMerger().canSkip()) {
        continue;
      }
      finalColumns.add(mergedDimension.getName());
      finalDimensions.add(mergedDimension.getName());
    }

    GenericIndexed<String> cols = GenericIndexed.fromIterable(finalColumns, GenericIndexed.STRING_STRATEGY);
    GenericIndexed<String> dims = GenericIndexed.fromIterable(finalDimensions, GenericIndexed.STRING_STRATEGY);

    final String bitmapSerdeFactoryType = mapper.writeValueAsString(indexSpec.getBitmapSerdeFactory());
    final long numBytes = cols.getSerializedSize()
                          + dims.getSerializedSize()
                          + 16
                          + serializerUtils.getSerializedStringByteSize(bitmapSerdeFactoryType);

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

    serializerUtils.writeLong(writer, dataInterval.getStartMillis());
    serializerUtils.writeLong(writer, dataInterval.getEndMillis());

    serializerUtils.writeString(writer, bitmapSerdeFactoryType);
    writer.close();

    IndexIO.checkFileSize(new File(outDir, "index.drd"));
    log.info("Completed index.drd in %,d millis.", System.currentTimeMillis() - startTime);

    progress.stopSection(section);
  }

  private void makeMetricsColumns(
      final FileSmoosher v9Smoosher,
      final ProgressIndicator progress,
      final List<MetricDesc> mergedMetrics,
      final List<GenericColumnSerializer> metWriters,
      final IndexSpec indexSpec
  ) throws IOException
  {
    final String section = "make metric columns";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < mergedMetrics.size(); ++i) {
      final MetricDesc metricDesc = mergedMetrics.get(i);
      long metricStartTime = System.currentTimeMillis();
      GenericColumnSerializer writer = metWriters.get(i);

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
      final ValueType type = metricDesc.getValueType();
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
          final String typeName = metricDesc.getType();
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
      makeColumn(v9Smoosher, metricDesc.getName(), builder.build());
      log.info(
          "Completed metric column[%s] in %,d millis.",
          metricDesc.getName(),
          System.currentTimeMillis() - metricStartTime
      );
    }
    log.info("Completed metric columns in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  static ColumnPartSerde createLongColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    // If using default values for null use LongGenericColumnPartSerde to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return LongGenericColumnPartSerde.serializerBuilder()
                                       .withByteOrder(IndexIO.BYTE_ORDER)
                                       .withDelegate(serializer)
                                       .build();
    } else {
      return LongGenericColumnPartSerdeV2.serializerBuilder()
                                         .withByteOrder(IndexIO.BYTE_ORDER)
                                         .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                         .withDelegate(serializer)
                                         .build();
    }
  }

  static ColumnPartSerde createDoubleColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    // If using default values for null use DoubleGenericColumnPartSerde to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return DoubleGenericColumnPartSerde.serializerBuilder()
                                         .withByteOrder(IndexIO.BYTE_ORDER)
                                         .withDelegate(serializer)
                                         .build();
    } else {
      return DoubleGenericColumnPartSerdeV2.serializerBuilder()
                                           .withByteOrder(IndexIO.BYTE_ORDER)
                                           .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                           .withDelegate(serializer)
                                           .build();
    }
  }

  static ColumnPartSerde createFloatColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    // If using default values for null use FloatGenericColumnPartSerde to allow rollback to previous versions.
    if (NullHandling.replaceWithDefault()) {
      return FloatGenericColumnPartSerde.serializerBuilder()
                                        .withByteOrder(IndexIO.BYTE_ORDER)
                                        .withDelegate(serializer)
                                        .build();
    } else {
      return FloatGenericColumnPartSerdeV2.serializerBuilder()
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
      final List<DimensionDesc> mergedDimensions,
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
        final DimensionMerger merger = mergedDimensions.get(dimIndex).getNonNullMerger();
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
        log.info("walked 500,000/%d rows in %,d millis.", rowCount, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
      }
    }
    if (rowNumConversions != null) {
      for (IntBuffer rowNumConversion : rowNumConversions) {
        rowNumConversion.rewind();
      }
    }
    log.info("completed walk through of %,d rows in %,d millis.", rowCount, System.currentTimeMillis() - startTime);
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
      final List<MetricDesc> mergedMetrics,
      final IndexSpec indexSpec
  ) throws IOException
  {
    ArrayList<GenericColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());

    for (MetricDesc metricDesc : mergedMetrics) {
      final ValueType type = metricDesc.getValueType();
      GenericColumnSerializer writer;
      switch (type) {
        case LONG:
          writer = createLongColumnSerializer(segmentWriteOutMedium, metricDesc.getName(), indexSpec);
          break;
        case FLOAT:
          writer = createFloatColumnSerializer(segmentWriteOutMedium, metricDesc.getName(), indexSpec);
          break;
        case DOUBLE:
          writer = createDoubleColumnSerializer(segmentWriteOutMedium, metricDesc.getName(), indexSpec);
          break;
        case COMPLEX:
          final String typeName = metricDesc.getType();
          ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Unknown type[%s]", typeName);
          }
          writer = serde.getSerializer(segmentWriteOutMedium, metricDesc.getName());
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
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression(),
          indexSpec.getLongEncoding()
      );
    } else {
      return LongColumnSerializerV2.create(
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
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression()
      );
    } else {
      return DoubleColumnSerializerV2.create(
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
          segmentWriteOutMedium,
          columnName,
          indexSpec.getMetricCompression()
      );
    } else {
      return FloatColumnSerializerV2.create(
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
      final List<DimensionDesc> mergedDimensions
  ) throws IOException
  {
    final String section = "setup dimension conversions";
    progress.startSection(section);

    for (DimensionDesc dimensionDesc : mergedDimensions) {
      dimensionDesc.getNonNullMerger().writeMergedValueDictionary(indexes);
    }
    progress.stopSection(section);
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

    FileUtils.forceMkdir(outDir);

    log.info("Starting persist for interval[%s], rows[%,d]", dataInterval, index.size());
    final IncrementalIndexAdapter adapter = new IncrementalIndexAdapter(
        dataInterval,
        index,
        indexSpec.getBitmapSerdeFactory().getBitmapFactory()
    );
    return merge(
        Collections.singletonList(adapter),
        false,
        index.getMetricAggs(),
        // if index is not rolled up, then it should be not rollup here
        // if index is rolled up, then it is no need to rollup again.
        //                     In this case, true/false won't cause reOrdering in merge stage
        //                     while merging a single iterable
        outDir,
        indexSpec,
        progress,
        1,
        segmentWriteOutMediumFactory
    ).getFile();
  }

  @Override
  public MergedIndexMetadata mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      int mergeDegree,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return merge(
        IndexMerger.toIndexableAdapters(indexes),
        rollup,
        metricAggs,
        outDir,
        indexSpec,
        new BaseProgressIndicator(),
        mergeDegree,
        segmentWriteOutMediumFactory
    );
  }

  @Override
  public MergedIndexMetadata mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      int mergeDegree,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return merge(
        IndexMerger.toIndexableAdapters(indexes),
        rollup,
        metricAggs,
        outDir,
        indexSpec,
        progress,
        mergeDegree,
        segmentWriteOutMediumFactory
    );
  }

  @Override
  public MergedIndexMetadata merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      int mergeDegree
  ) throws IOException
  {
    return merge(indexes, rollup, metricAggs, outDir, indexSpec, new BaseProgressIndicator(), mergeDegree, null);
  }

  private MergedIndexMetadata merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      int mergeDegree,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    FileUtils.forceMkdir(outDir);

    final List<DimensionDesc> mergedDimensions = IndexMerger.mergeDimensions(indexes);
    final List<MetricDesc> mergedMetrics = IndexMerger.mergeMetrics(indexes, metricAggs);

    final Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn;
    if (rollup) {
      rowMergerFn = rowIterators -> new RowCombiningTimeAndDimsIterator(rowIterators, mergedMetrics);
    } else {
      rowMergerFn = MergingRowIterator::new;
    }

    return new MergedIndexMetadata(
        makeIndexFiles(
            indexes,
            outDir,
            progress,
            mergedDimensions,
            mergedMetrics,
            rowMergerFn,
            true,
            indexSpec,
            mergeDegree,
            segmentWriteOutMediumFactory
        ),
        mergedDimensions.stream().map(DimensionDesc::getName).collect(Collectors.toList())
    );
  }

  private static class MergeOutDirSupplier implements Supplier<File>
  {
    private final File baseDir;
    private int nextSuffix = 0;

    MergeOutDirSupplier(File baseDir)
    {
      this.baseDir = baseDir;
    }

    @Override
    public File get()
    {
      return createFile(nextSuffix++);
    }

    public File getLast()
    {
      if (nextSuffix == 0) {
        throw new ISE("there's no valid last outDir");
      } else {
        return createFile(nextSuffix - 1);
      }
    }

    private File createFile(int suffix)
    {
      return new File(baseDir, StringUtils.format("interm_merge_%d", suffix));
    }
  }

  @Override
  public File convert(final File inDir, final File outDir, final IndexSpec indexSpec) throws IOException
  {
    return convert(inDir, outDir, indexSpec, new BaseProgressIndicator(), defaultSegmentWriteOutMediumFactory);
  }

  @Override
  public File convert(
      final File inDir,
      final File outDir,
      final IndexSpec indexSpec,
      final ProgressIndicator progress,
      final @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    try (QueryableIndex index = indexIO.loadIndex(inDir)) {
      final IndexableAdapter adapter = new QueryableIndexAdapter(index);
      return makeIndexFiles(
          ImmutableList.of(adapter),
          outDir,
          progress,
          IndexMerger.mergeDimensions(Collections.singletonList(adapter)),
          IndexMerger.mergeMetrics(Collections.singletonList(adapter), null),
          Iterables::getOnlyElement,
          false,
          indexSpec,
          1,
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
    FileUtils.forceMkdir(outDir);

    final List<DimensionDesc> mergedDimensions = IndexMerger.mergeDimensions(indexes);
    final List<MetricDesc> mergedMetrics = IndexMerger.mergeMetrics(indexes, aggregators);

    return makeIndexFiles(
        indexes,
        outDir,
        new BaseProgressIndicator(),
        mergedDimensions,
        mergedMetrics,
        MergingRowIterator::new,
        true,
        indexSpec,
        1,
        segmentWriteOutMediumFactory
    );
  }

  private TimeAndDimsIterator makeMergedTimeAndDimsIterator(
      final List<IndexableAdapter> indexes,
      final List<DimensionDesc> mergedDimensions,
      final List<MetricDesc> mergedMetrics,
      final Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn
  )
  {
    final List<TransformableRowIterator> perIndexRowIterators = Lists.newArrayListWithCapacity(indexes.size());
    final List<String> mergedDimensionNames = mergedDimensions.stream()
                                                              .map(DimensionDesc::getName)
                                                              .collect(Collectors.toList());
    final Map<String, DimensionHandler> handlers = mergedDimensions
        .stream()
        .collect(Collectors.toMap(DimensionDesc::getName, DimensionDesc::getHandler));
    final List<DimensionMerger> mergers = mergedDimensions.stream()
                                                          .map(DimensionDesc::getNonNullMerger)
                                                          .collect(Collectors.toList());
    final List<String> mergedMetricNames = mergedMetrics.stream()
                                                        .map(MetricDesc::getName)
                                                        .collect(Collectors.toList());
    for (int i = 0; i < indexes.size(); ++i) {
      final IndexableAdapter adapter = indexes.get(i);
      TransformableRowIterator target = adapter.getRows();
      if (!mergedDimensionNames.equals(adapter.getDimensionNames())
          || !mergedMetricNames.equals(adapter.getMetricNames())) {
        target = makeRowIteratorWithReorderedColumns(
            mergedDimensionNames,
            mergedMetricNames,
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
