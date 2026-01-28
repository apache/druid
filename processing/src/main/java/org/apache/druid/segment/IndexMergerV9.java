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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.apache.druid.io.ZeroCopyByteArrayOutputStream;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.serde.NullColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.utils.CloseableUtils;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link IndexMerger} for creating v9 format segments with {@link FileSmoosher}
 *
 * @see FileSmoosher
 * @see org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper to read the resulting segment files
 */
public class IndexMergerV9 extends IndexMergerBase
{
  private static final Logger log = new Logger(IndexMergerV9.class);

  /**
   * This is a flag to enable storing empty columns in the segments.
   * It exists today only just in case where there is an unknown bug in storing empty columns.
   * We will get rid of it eventually in the near future.
   */
  private final boolean storeEmptyColumns;

  public IndexMergerV9(
      ObjectMapper mapper,
      IndexIO indexIO,
      SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory,
      boolean storeEmptyColumns
  )
  {
    super(mapper, indexIO, defaultSegmentWriteOutMediumFactory);
    this.storeEmptyColumns = storeEmptyColumns;
  }

  /**
   * This constructor is used only for Hadoop ingestion and Tranquility as they do not support storing empty columns yet.
   * See {@code HadoopDruidIndexerConfig} and {@code PlumberSchool} for hadoop ingestion and Tranquility, respectively.
   */
  @Inject
  public IndexMergerV9(
      ObjectMapper mapper,
      IndexIO indexIO,
      SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory
  )
  {
    this(mapper, indexIO, defaultSegmentWriteOutMediumFactory, false);
  }

  @Override
  protected boolean shouldStoreEmptyColumns()
  {
    return storeEmptyColumns;
  }

  @Override
  protected File makeIndexFiles(
      final List<IndexableAdapter> adapters,
      final @Nullable Metadata segmentMetadata,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedDimensionsWithTime, // has both explicit and implicit dimensions, as well as __time
      final DimensionsSpecInspector dimensionsSpecInspector,
      final List<String> mergedMetrics,
      final Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      final IndexSpec indexSpec,
      final @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    progress.start();
    progress.progress();

    // Merged dimensions without __time.
    List<String> mergedDimensions =
        mergedDimensionsWithTime.stream()
                                .filter(dim -> !ColumnHolder.TIME_COLUMN_NAME.equals(dim))
                                .collect(Collectors.toList());
    Closer closer = Closer.create();
    final FileSmoosher v9Smoosher = new FileSmoosher(outDir);
    try {

      FileUtils.mkdirp(outDir);

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
      final Map<String, ColumnFormat> metricFormats = new TreeMap<>(Comparators.naturalNullsFirst());
      final List<ColumnFormat> dimFormats = Lists.newArrayListWithCapacity(mergedDimensions.size());
      mergeFormat(adapters, mergedDimensions, metricFormats, dimFormats);

      final Map<String, DimensionHandler> handlers = makeDimensionHandlers(mergedDimensions, dimFormats);
      final Map<String, DimensionMergerV9> mergersMap = Maps.newHashMapWithExpectedSize(mergedDimensions.size());
      final List<DimensionMergerV9> mergers = new ArrayList<>();
      for (int i = 0; i < mergedDimensions.size(); i++) {
        DimensionHandler handler = handlers.get(mergedDimensions.get(i));
        DimensionMergerV9 merger = handler.makeMerger(
            mergedDimensions.get(i),
            indexSpec,
            segmentWriteOutMedium,
            dimFormats.get(i).toColumnCapabilities(),
            progress,
            outDir,
            closer
        );
        mergers.add(merger);
        mergersMap.put(mergedDimensions.get(i), merger);
      }

      if (segmentMetadata != null && segmentMetadata.getProjections() != null) {
        for (AggregateProjectionMetadata projectionMetadata : segmentMetadata.getProjections()) {
          for (String dimension : projectionMetadata.getSchema().getGroupingColumns()) {
            DimensionMergerV9 merger = mergersMap.get(dimension);
            if (merger != null) {
              merger.markAsParent();
            }
          }
        }
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
          mergedDimensionsWithTime,
          mergedMetrics,
          rowMergerFn,
          handlers,
          mergers
      );
      closer.register(timeAndDimsIterator);
      final GenericColumnSerializer timeWriter = setupTimeWriter(segmentWriteOutMedium, indexSpec);
      final ArrayList<GenericColumnSerializer> metricWriters =
          setupMetricsWriters(segmentWriteOutMedium, mergedMetrics, metricFormats, indexSpec, "");
      IndexMergeResult indexMergeResult = mergeIndexesAndWriteColumns(
          adapters,
          progress,
          timeAndDimsIterator,
          timeWriter,
          metricWriters,
          mergers
      );

      /************ Create Inverted Indexes and Finalize Build Columns *************/
      final String section = "build inverted index and columns";
      progress.startSection(section);
      makeTimeColumn(v9Smoosher, progress, timeWriter, indexSpec, ColumnHolder.TIME_COLUMN_NAME);
      makeMetricsColumns(
          v9Smoosher,
          progress,
          mergedMetrics,
          metricFormats,
          metricWriters,
          indexSpec,
          ""
      );

      for (int i = 0; i < mergedDimensions.size(); i++) {
        DimensionMergerV9 merger = mergers.get(i);
        merger.writeIndexes(indexMergeResult.rowNumConversions);
        if (!merger.hasOnlyNulls()) {
          ColumnDescriptor columnDesc = merger.makeColumnDescriptor();
          makeColumn(v9Smoosher, mergedDimensions.get(i), columnDesc);
        } else if (dimensionsSpecInspector.shouldStore(mergedDimensions.get(i))) {
          // shouldStore AND hasOnlyNulls
          ColumnDescriptor columnDesc = ColumnDescriptor
              .builder()
              .setValueType(dimFormats.get(i).getLogicalType().getType())
              .addSerde(new NullColumnPartSerde(indexMergeResult.rowCount, indexSpec.getBitmapSerdeFactory()))
              .build();
          makeColumn(v9Smoosher, mergedDimensions.get(i), columnDesc);
        }
      }

      progress.stopSection(section);

      // Recompute the projections.
      final Metadata finalMetadata;
      if (segmentMetadata == null || CollectionUtils.isNullOrEmpty(segmentMetadata.getProjections())) {
        finalMetadata = segmentMetadata;
      } else {
        finalMetadata = makeProjections(
            v9Smoosher,
            segmentMetadata.getProjections(),
            adapters,
            indexSpec,
            segmentWriteOutMedium,
            progress,
            outDir,
            closer,
            mergersMap,
            segmentMetadata
        );
      }

      /************* Make index.drd & metadata.drd files **************/
      progress.progress();
      makeIndexBinary(
          v9Smoosher,
          adapters,
          outDir,
          mergedDimensions,
          mergedMetrics,
          progress,
          indexSpec,
          mergers,
          dimensionsSpecInspector
      );
      makeMetadataBinary(v9Smoosher, progress, finalMetadata);

      v9Smoosher.close();
      progress.stop();

      return outDir;
    }
    catch (Throwable t) {
      CloseableUtils.closeAndWrapExceptions(v9Smoosher::abort);
      throw closer.rethrow(t);
    }
    finally {
      closer.close();
    }
  }

  @Override
  protected void makeColumn(
      final SegmentFileBuilder segmentFileBuilder,
      final String columnName,
      final ColumnDescriptor serdeficator
  ) throws IOException
  {
    ZeroCopyByteArrayOutputStream specBytes = new ZeroCopyByteArrayOutputStream();
    SERIALIZER_UTILS.writeString(specBytes, mapper.writeValueAsString(serdeficator));
    try (SegmentFileChannel channel = segmentFileBuilder.addWithChannel(
        columnName,
        specBytes.size() + serdeficator.getSerializedSize()
    )) {
      specBytes.writeTo(channel);
      serdeficator.writeTo(channel, segmentFileBuilder);
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
      final List<DimensionMergerV9> mergers,
      final DimensionsSpecInspector dimensionsSpecInspector
  ) throws IOException
  {
    final Set<String> columnSet = new HashSet<>(mergedDimensions);
    columnSet.addAll(mergedMetrics);
    Preconditions.checkState(
        columnSet.size() == mergedDimensions.size() + mergedMetrics.size(),
        "column names are not unique in dims[%s] and mets[%s]",
        mergedDimensions,
        mergedMetrics
    );

    final String section = "make index.drd";
    progress.startSection(section);

    long startTime = System.currentTimeMillis();

    // The original column order is encoded in the below arrayLists.
    // At the positions where there is a non-null column in uniqueDims/uniqueMets,
    // null is stored instead of actual column name. At other positions, the name of null columns are stored.
    // When the segment is loaded, original column order is restored
    // by merging nonNullOnlyColumns/nonNullOnlyDimensions and allColumns/allDimensions.
    // See V9IndexLoader.restoreColumns() for more details of how the original order is restored.
    final List<String> nonNullOnlyDimensions = new ArrayList<>(mergedDimensions.size());
    final List<String> nonNullOnlyColumns = new ArrayList<>(mergedDimensions.size() + mergedMetrics.size());
    nonNullOnlyColumns.addAll(mergedMetrics);
    final List<String> allDimensions = new ArrayList<>(mergedDimensions.size());
    final List<String> allColumns = new ArrayList<>(mergedDimensions.size() + mergedMetrics.size());
    IntStream.range(0, mergedMetrics.size()).forEach(i -> allColumns.add(null));
    for (int i = 0; i < mergedDimensions.size(); ++i) {
      if (!mergers.get(i).hasOnlyNulls()) {
        nonNullOnlyDimensions.add(mergedDimensions.get(i));
        nonNullOnlyColumns.add(mergedDimensions.get(i));
        allDimensions.add(null);
        allColumns.add(null);
      } else if (dimensionsSpecInspector.shouldStore(mergedDimensions.get(i))) {
        // shouldStore AND hasOnlyNulls
        allDimensions.add(mergedDimensions.get(i));
        allColumns.add(mergedDimensions.get(i));
      }
    }

    GenericIndexed<String> nonNullCols = GenericIndexed.fromIterable(
        nonNullOnlyColumns,
        GenericIndexed.STRING_STRATEGY
    );
    GenericIndexed<String> nonNullDims = GenericIndexed.fromIterable(
        nonNullOnlyDimensions,
        GenericIndexed.STRING_STRATEGY
    );
    GenericIndexed<String> nullCols = GenericIndexed.fromIterable(allColumns, GenericIndexed.STRING_STRATEGY);
    GenericIndexed<String> nullDims = GenericIndexed.fromIterable(
        allDimensions,
        GenericIndexed.STRING_STRATEGY
    );

    final String bitmapSerdeFactoryType = mapper.writeValueAsString(indexSpec.getBitmapSerdeFactory());
    final long numBytes = nonNullCols.getSerializedSize()
                          + nonNullDims.getSerializedSize()
                          + nullCols.getSerializedSize()
                          + nullDims.getSerializedSize()
                          + 16
                          + SERIALIZER_UTILS.getSerializedStringByteSize(bitmapSerdeFactoryType);

    try (final SegmentFileChannel writer = v9Smoosher.addWithChannel("index.drd", numBytes)) {
      nonNullCols.writeTo(writer, v9Smoosher);
      nonNullDims.writeTo(writer, v9Smoosher);

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

      // Store null-only dimensions at the end of this section,
      // so that historicals of an older version can ignore them instead of exploding while reading this segment.
      // Those historicals will still serve any query that reads null-only columns.
      nullCols.writeTo(writer, v9Smoosher);
      nullDims.writeTo(writer, v9Smoosher);
    }

    IndexIO.checkFileSize(new File(outDir, "index.drd"));
    log.debug("Completed index.drd in %,d millis.", System.currentTimeMillis() - startTime);

    progress.stopSection(section);
  }
}
