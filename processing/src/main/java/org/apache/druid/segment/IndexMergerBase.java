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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.serde.DoubleNumericColumnPartSerdeV2;
import org.apache.druid.segment.serde.FloatNumericColumnPartSerdeV2;
import org.apache.druid.segment.serde.LongNumericColumnPartSerdeV2;
import org.apache.druid.segment.serde.NullColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shared segment building functionality for segments built by merging {@link IndexableAdapter}
 */
public abstract class IndexMergerBase implements IndexMerger
{
  protected final ObjectMapper mapper;
  protected final IndexIO indexIO;
  protected final SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory;

  protected IndexMergerBase(
      ObjectMapper mapper,
      IndexIO indexIO,
      SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory
  )
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");
    this.defaultSegmentWriteOutMediumFactory =
        Preconditions.checkNotNull(defaultSegmentWriteOutMediumFactory, "null SegmentWriteOutMediumFactory");
  }

  /**
   * Should the segment track columns which were specified in the ingestion spec, but not present in the processed data?
   */
  protected abstract boolean shouldStoreEmptyColumns();

  /**
   * Merge {@link IndexableAdapter} and build the segment file
   */
  protected abstract File makeIndexFiles(
      List<IndexableAdapter> adapters,
      @Nullable Metadata segmentMetadata,
      File outDir,
      ProgressIndicator progress,
      List<String> mergedDimensionsWithTime, // has both explicit and implicit dimensions, as well as __time
      DimensionsSpecInspector dimensionsSpecInspector,
      List<String> mergedMetrics,
      Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  /**
   * Add a column specified by a {@link ColumnDescriptor} to a {@link SegmentFileBuilder}
   */
  protected abstract void makeColumn(
      SegmentFileBuilder segmentFileBuilder,
      String columnName,
      ColumnDescriptor serdeficator
  ) throws IOException;

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

    indexSpec = indexSpec.getEffectiveSpec();

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

    FileUtils.mkdirp(outDir);

    log.debug("Starting persist for interval[%s], rows[%,d]", dataInterval, index.numRows());
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
        index.getDimensionsSpec(),
        outDir,
        indexSpec,
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
      @Nullable DimensionsSpec dimensionsSpec,
      File outDir,
      IndexSpec indexSpec,
      IndexSpec indexSpecForIntermediatePersists,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException
  {
    return multiphaseMerge(
        IndexMerger.toIndexableAdapters(indexes),
        rollup,
        metricAggs,
        dimensionsSpec,
        outDir,
        indexSpec,
        indexSpecForIntermediatePersists,
        progress,
        segmentWriteOutMediumFactory,
        maxColumnsToMerge
    );
  }

  /**
   * The indexes here must have the same {@link Metadata}, otherwise an error would be thrown.
   */
  @Override
  public File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      final AggregatorFactory[] metricAggs,
      File outDir,
      DimensionsSpec dimensionsSpec,
      IndexSpec indexSpec,
      int maxColumnsToMerge
  ) throws IOException
  {
    return multiphaseMerge(
        indexes,
        rollup,
        metricAggs,
        dimensionsSpec,
        outDir,
        indexSpec,
        indexSpec,
        new BaseProgressIndicator(),
        null,
        maxColumnsToMerge
    );
  }

  protected File multiphaseMerge(
      List<IndexableAdapter> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      @Nullable DimensionsSpec dimensionsSpec,
      File outDir,
      IndexSpec indexSpec,
      IndexSpec indexSpecForIntermediatePersists,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    FileUtils.mkdirp(outDir);

    List<File> tempDirs = new ArrayList<>();

    indexSpec = indexSpec.getEffectiveSpec();
    indexSpecForIntermediatePersists = indexSpecForIntermediatePersists.getEffectiveSpec();
    log.info("Building segment with IndexSpec[%s]", indexSpec);

    if (maxColumnsToMerge == IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE) {
      return merge(
          indexes,
          rollup,
          metricAggs,
          dimensionsSpec,
          outDir,
          indexSpec,
          progress,
          segmentWriteOutMediumFactory
      );
    }

    List<List<IndexableAdapter>> currentPhases = getMergePhases(indexes, maxColumnsToMerge);
    List<File> currentOutputs = new ArrayList<>();

    log.debug("Base outDir[%s]", outDir);

    try {
      int tierCounter = 0;
      while (true) {
        log.info(
            "Merging phases[%,d] (indexes[%,d], maxColumnsToMerge[%,d]), tiers finished processed so far[%,d].",
            currentPhases.size(),
            indexes.size(),
            maxColumnsToMerge,
            tierCounter
        );
        for (List<IndexableAdapter> phase : currentPhases) {
          final File phaseOutDir;
          final boolean isFinalPhase = currentPhases.size() == 1;
          if (isFinalPhase) {
            // use the given outDir on the final merge phase
            phaseOutDir = outDir;
            log.info("Performing final merge phase.");
          } else {
            phaseOutDir = FileUtils.createTempDir();
            tempDirs.add(phaseOutDir);
          }
          log.info("Merging phase with index count[%,d].", phase.size());
          log.debug("Phase outDir[%s]", phaseOutDir);

          File phaseOutput = merge(
              phase,
              rollup,
              metricAggs,
              dimensionsSpec,
              phaseOutDir,
              isFinalPhase ? indexSpec : indexSpecForIntermediatePersists,
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

  protected List<List<IndexableAdapter>> getMergePhases(List<IndexableAdapter> indexes, int maxColumnsToMerge)
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

  protected int getIndexColumnCount(IndexableAdapter indexableAdapter)
  {
    return indexableAdapter.getDimensionNames(true).size() + indexableAdapter.getMetricNames().size();
  }

  protected int getIndexColumnCount(List<IndexableAdapter> indexableAdapters)
  {
    int count = 0;
    for (IndexableAdapter indexableAdapter : indexableAdapters) {
      count += getIndexColumnCount(indexableAdapter);
    }
    return count;
  }

  protected File merge(
      List<IndexableAdapter> indexes,
      final boolean rollup,
      final AggregatorFactory[] metricAggs,
      @Nullable DimensionsSpec dimensionsSpec,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    final List<String> mergedDimensionsWithTime = IndexMerger.getMergedDimensionsWithTime(indexes, dimensionsSpec);

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

    List<Metadata> metadataList = Lists.transform(indexes, IndexableAdapter::getMetadata);
    AggregatorFactory[] combiningMetricAggs = new AggregatorFactory[sortedMetricAggs.length];
    for (int i = 0; i < sortedMetricAggs.length; i++) {
      combiningMetricAggs[i] = sortedMetricAggs[i].getCombiningFactory();
    }
    final Metadata segmentMetadata = Metadata.merge(metadataList, combiningMetricAggs);

    if (segmentMetadata != null
        && segmentMetadata.getOrdering() != null
        && segmentMetadata.getOrdering()
                          .stream()
                          .noneMatch(orderBy -> ColumnHolder.TIME_COLUMN_NAME.equals(orderBy.getColumnName()))) {
      throw DruidException.defensive(
          "sortOrder[%s] must include[%s]",
          segmentMetadata.getOrdering(),
          ColumnHolder.TIME_COLUMN_NAME
      );
    }

    return makeIndexFiles(
        indexes,
        segmentMetadata,
        outDir,
        progress,
        mergedDimensionsWithTime,
        new DimensionsSpecInspector(shouldStoreEmptyColumns(), dimensionsSpec),
        mergedMetrics,
        rowMergerFn,
        indexSpec,
        segmentWriteOutMediumFactory
    );
  }

  protected Metadata makeProjections(
      final SegmentFileBuilder segmentFileBuilder,
      final List<AggregateProjectionMetadata> projections,
      final List<IndexableAdapter> adapters,
      final IndexSpec indexSpec,
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final ProgressIndicator progress,
      final File segmentBaseDir,
      final Closer closer,
      final Map<String, DimensionMergerV9> parentMergers,
      final Metadata segmentMetadata
  ) throws IOException
  {
    final List<AggregateProjectionMetadata> projectionMetadata = Lists.newArrayListWithCapacity(projections.size());
    for (AggregateProjectionMetadata spec : projections) {
      final List<IndexableAdapter> projectionAdapters = Lists.newArrayListWithCapacity(adapters.size());
      final AggregateProjectionSchema projectionSchema = spec.getSchema();
      for (IndexableAdapter adapter : adapters) {
        projectionAdapters.add(adapter.getProjectionAdapter(projectionSchema.getName()));
      }
      // we can use the first adapter to get the dimensions and metrics because the projection schema should be
      // identical across all segments. This is validated by segment metadata merging
      final List<String> dimensions = projectionAdapters.get(0).getDimensionNames(false);
      final List<String> metrics = Arrays.stream(projectionSchema.getAggregators())
                                         .map(AggregatorFactory::getName)
                                         .collect(Collectors.toList());


      final List<DimensionMergerV9> mergers = new ArrayList<>();
      final Map<String, ColumnFormat> columnFormats = Maps.newLinkedHashMapWithExpectedSize(dimensions.size()
                                                                                            + metrics.size());

      for (String dimension : dimensions) {
        final ColumnFormat dimensionFormat = projectionAdapters.get(0).getFormat(dimension);
        columnFormats.put(dimension, dimensionFormat);
        DimensionHandler handler = dimensionFormat.getColumnHandler(dimension);
        DimensionMergerV9 merger = handler.makeMerger(
            Projections.getProjectionSmooshFileName(spec.getSchema(), dimension),
            indexSpec,
            segmentWriteOutMedium,
            dimensionFormat.toColumnCapabilities(),
            progress,
            segmentBaseDir,
            closer
        );
        if (parentMergers.containsKey(dimension)) {
          merger.attachParent(parentMergers.get(dimension), projectionAdapters);
        } else {
          merger.writeMergedValueDictionary(projectionAdapters);
        }
        mergers.add(merger);
      }
      for (String metric : metrics) {
        columnFormats.put(metric, projectionAdapters.get(0).getFormat(metric));
      }

      final GenericColumnSerializer timeWriter;
      if (projectionSchema.getTimeColumnName() != null) {
        timeWriter = setupTimeWriter(segmentWriteOutMedium, indexSpec);
      } else {
        timeWriter = null;
      }
      final ArrayList<GenericColumnSerializer> metricWriters =
          setupMetricsWriters(
              segmentWriteOutMedium,
              metrics,
              columnFormats,
              indexSpec,
              Projections.getProjectionSmooshPrefix(spec.getSchema())
          );

      Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn =
          rowIterators -> new RowCombiningTimeAndDimsIterator(rowIterators, projectionSchema.getAggregators(), metrics);

      List<TransformableRowIterator> perIndexRowIterators = Lists.newArrayListWithCapacity(projectionAdapters.size());
      for (int i = 0; i < projectionAdapters.size(); ++i) {
        final IndexableAdapter adapter = projectionAdapters.get(i);
        TransformableRowIterator target = adapter.getRows();
        perIndexRowIterators.add(IndexMerger.toMergedIndexRowIterator(target, i, mergers));
      }
      final TimeAndDimsIterator timeAndDimsIterator = rowMergerFn.apply(perIndexRowIterators);
      closer.register(timeAndDimsIterator);

      int rowCount = 0;
      List<IntBuffer> rowNumConversions = new ArrayList<>(projectionAdapters.size());
      for (IndexableAdapter adapter : projectionAdapters) {
        int[] arr = new int[adapter.getNumRows()];
        Arrays.fill(arr, INVALID_ROW);
        rowNumConversions.add(IntBuffer.wrap(arr));
      }

      final String section = "walk through and merge projection[" + projectionSchema.getName() + "] rows";
      progress.startSection(section);
      long startTime = System.currentTimeMillis();
      long time = startTime;
      while (timeAndDimsIterator.moveToNext()) {
        progress.progress();
        TimeAndDimsPointer timeAndDims = timeAndDimsIterator.getPointer();
        if (timeWriter != null) {
          timeWriter.serialize(timeAndDims.timestampSelector);
        }

        for (int metricIndex = 0; metricIndex < timeAndDims.getNumMetrics(); metricIndex++) {
          metricWriters.get(metricIndex).serialize(timeAndDims.getMetricSelector(metricIndex));
        }

        for (int dimIndex = 0; dimIndex < timeAndDims.getNumDimensions(); dimIndex++) {
          DimensionMergerV9 merger = mergers.get(dimIndex);
          if (merger.hasOnlyNulls()) {
            continue;
          }
          merger.processMergedRow(timeAndDims.getDimensionSelector(dimIndex));
        }

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
        if ((++rowCount % 500000) == 0) {
          log.debug(
              "walked 500,000/%d rows of projection[%s] in %,d millis.",
              rowCount,
              projectionSchema.getName(),
              System.currentTimeMillis() - time
          );
          time = System.currentTimeMillis();
        }
      }
      for (IntBuffer rowNumConversion : rowNumConversions) {
        rowNumConversion.rewind();
      }
      log.debug(
          "completed walk through of %,d rows of projection[%s] in %,d millis.",
          rowCount,
          projectionSchema.getName(),
          System.currentTimeMillis() - startTime
      );
      progress.stopSection(section);

      final String section2 = "build projection[" + projectionSchema.getName() + "] inverted index and columns";
      progress.startSection(section2);
      if (projectionSchema.getTimeColumnName() != null) {
        makeTimeColumn(
            segmentFileBuilder,
            progress,
            timeWriter,
            indexSpec,
            Projections.getProjectionSmooshFileName(spec.getSchema(), projectionSchema.getTimeColumnName())
        );
      }
      makeMetricsColumns(
          segmentFileBuilder,
          progress,
          metrics,
          columnFormats,
          metricWriters,
          indexSpec,
          Projections.getProjectionSmooshPrefix(spec.getSchema())
      );

      for (int i = 0; i < dimensions.size(); i++) {
        final String dimension = dimensions.get(i);
        final DimensionMergerV9 merger = mergers.get(i);
        merger.writeIndexes(rowNumConversions);
        final ColumnDescriptor columnDesc;
        if (merger.hasOnlyNulls()) {
          columnDesc = ColumnDescriptor
              .builder()
              .setValueType(columnFormats.get(dimension).getLogicalType().getType())
              .addSerde(new NullColumnPartSerde(rowCount, indexSpec.getBitmapSerdeFactory()))
              .build();
        } else {
          // use merger descriptor, merger either has values or handles it own null column storage details
          columnDesc = merger.makeColumnDescriptor();
        }
        makeColumn(segmentFileBuilder, Projections.getProjectionSmooshFileName(spec.getSchema(), dimension), columnDesc);
      }

      progress.stopSection(section2);
      projectionMetadata.add(new AggregateProjectionMetadata(projectionSchema, rowCount));
    }
    return segmentMetadata.withProjections(projectionMetadata);
  }

  protected void makeTimeColumn(
      final SegmentFileBuilder segmentFileBuilder,
      final ProgressIndicator progress,
      final GenericColumnSerializer timeWriter,
      final IndexSpec indexSpec,
      final String name
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
    makeColumn(segmentFileBuilder, name, serdeficator);
    log.debug("Completed time column in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  protected void makeMetricsColumns(
      final SegmentFileBuilder segmentFileBuilder,
      final ProgressIndicator progress,
      final List<String> mergedMetrics,
      final Map<String, ColumnFormat> metricsTypes,
      final List<GenericColumnSerializer> metWriters,
      final IndexSpec indexSpec,
      final String namePrefix
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
      TypeSignature<ValueType> type = metricsTypes.get(metric).getLogicalType();
      switch (type.getType()) {
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
          final String typeName = type.getComplexTypeName();
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
      final String columnName = namePrefix + metric;
      makeColumn(segmentFileBuilder, columnName, builder.build());
      log.debug("Completed metric column[%s] in %,d millis.", columnName, System.currentTimeMillis() - metricStartTime);
    }
    log.debug("Completed metric columns in %,d millis.", System.currentTimeMillis() - startTime);
    progress.stopSection(section);
  }

  /**
   * Returns rowNumConversions, if fillRowNumConversions argument is true
   */
  protected IndexMergeResult mergeIndexesAndWriteColumns(
      final List<IndexableAdapter> adapters,
      final ProgressIndicator progress,
      final TimeAndDimsIterator timeAndDimsIterator,
      final GenericColumnSerializer timeWriter,
      final ArrayList<GenericColumnSerializer> metricWriters,
      final List<DimensionMergerV9> mergers
  ) throws IOException
  {
    final String section = "walk through and merge rows";
    progress.startSection(section);
    long startTime = System.currentTimeMillis();

    int rowCount = 0;
    List<IntBuffer> rowNumConversions = new ArrayList<>(adapters.size());
    for (IndexableAdapter adapter : adapters) {
      int[] arr = new int[adapter.getNumRows()];
      Arrays.fill(arr, INVALID_ROW);
      rowNumConversions.add(IntBuffer.wrap(arr));
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
        DimensionMergerV9 merger = mergers.get(dimIndex);
        if (merger.hasOnlyNulls()) {
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
        throw new IllegalStateException(
            "Filling row num conversions is supported only with RowCombining and Merging iterators"
        );
      }

      if ((++rowCount % 500000) == 0) {
        log.debug("walked 500,000/%d rows in %,d millis.", rowCount, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
      }
    }
    for (IntBuffer rowNumConversion : rowNumConversions) {
      rowNumConversion.rewind();
    }
    log.debug("completed walk through of %,d rows in %,d millis.", rowCount, System.currentTimeMillis() - startTime);
    progress.stopSection(section);
    return new IndexMergeResult(rowNumConversions, rowCount);
  }

  protected GenericColumnSerializer setupTimeWriter(
      SegmentWriteOutMedium segmentWriteOutMedium,
      IndexSpec indexSpec
  ) throws IOException
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

  protected ArrayList<GenericColumnSerializer> setupMetricsWriters(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final List<String> mergedMetrics,
      final Map<String, ColumnFormat> metricsTypes,
      final IndexSpec indexSpec,
      final String prefix
  ) throws IOException
  {
    ArrayList<GenericColumnSerializer> metWriters = Lists.newArrayListWithCapacity(mergedMetrics.size());

    for (String metric : mergedMetrics) {
      TypeSignature<ValueType> type = metricsTypes.get(metric).getLogicalType();
      final String outputName = prefix + metric;
      GenericColumnSerializer writer;
      switch (type.getType()) {
        case LONG:
          writer = createLongColumnSerializer(segmentWriteOutMedium, outputName, indexSpec);
          break;
        case FLOAT:
          writer = createFloatColumnSerializer(segmentWriteOutMedium, outputName, indexSpec);
          break;
        case DOUBLE:
          writer = createDoubleColumnSerializer(segmentWriteOutMedium, outputName, indexSpec);
          break;
        case COMPLEX:
          ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type.getComplexTypeName());
          if (serde == null) {
            throw new ISE("Unknown type[%s]", type.getComplexTypeName());
          }
          writer = serde.getSerializer(segmentWriteOutMedium, outputName, indexSpec);
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

  protected void writeDimValuesAndSetupDimConversion(
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

  protected void mergeFormat(
      final List<IndexableAdapter> adapters,
      final List<String> mergedDimensions,
      final Map<String, ColumnFormat> metricTypes,
      final List<ColumnFormat> dimFormats
  )
  {
    final Map<String, ColumnFormat> columnFormats = new HashMap<>();
    for (IndexableAdapter adapter : adapters) {
      for (String dimension : adapter.getDimensionNames(false)) {
        ColumnFormat format = adapter.getFormat(dimension);
        columnFormats.compute(
            dimension,
            (d, existingFormat) -> existingFormat == null ? format : format.merge(existingFormat)
        );
      }
      for (String metric : adapter.getMetricNames()) {
        final ColumnFormat format = adapter.getFormat(metric);
        final ColumnFormat merged = columnFormats.compute(
            metric,
            (m, existingFormat) ->
                existingFormat == null ? format : format.merge(existingFormat)
        );

        metricTypes.put(metric, merged);
      }
    }
    for (String dim : mergedDimensions) {
      dimFormats.add(columnFormats.get(dim));
    }
  }

  protected Map<String, DimensionHandler> makeDimensionHandlers(
      final List<String> mergedDimensions,
      final List<ColumnFormat> dimFormats
  )
  {
    Map<String, DimensionHandler> handlers = new LinkedHashMap<>();
    for (int i = 0; i < mergedDimensions.size(); i++) {
      String dimName = mergedDimensions.get(i);
      DimensionHandler handler = dimFormats.get(i).getColumnHandler(dimName);
      handlers.put(dimName, handler);
    }
    return handlers;
  }

  protected TimeAndDimsIterator makeMergedTimeAndDimsIterator(
      final List<IndexableAdapter> indexes,
      final List<String> mergedDimensionsWithTime,
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
      if (!mergedDimensionsWithTime.equals(adapter.getDimensionNames(true))
          || !mergedMetrics.equals(adapter.getMetricNames())) {
        target = makeRowIteratorWithReorderedColumns(
            mergedDimensionsWithTime,
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
      List<String> reorderedDimensionsWithTime,
      List<String> reorderedMetrics,
      Map<String, DimensionHandler> originalHandlers,
      IndexableAdapter originalAdapter,
      TransformableRowIterator originalIterator
  )
  {
    RowPointer reorderedRowPointer = reorderRowPointerColumns(
        reorderedDimensionsWithTime,
        reorderedMetrics,
        originalHandlers,
        originalAdapter,
        originalIterator.getPointer()
    );
    TimeAndDimsPointer reorderedMarkedRowPointer = reorderRowPointerColumns(
        reorderedDimensionsWithTime,
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
      List<String> reorderedDimensionsWithTime,
      List<String> reorderedMetrics,
      Map<String, DimensionHandler> originalHandlers,
      IndexableAdapter originalAdapter,
      T originalRowPointer
  )
  {
    int reorderedTimePosition = reorderedDimensionsWithTime.indexOf(ColumnHolder.TIME_COLUMN_NAME);
    if (reorderedTimePosition < 0) {
      throw DruidException.defensive("Missing column[%s]", ColumnHolder.TIME_COLUMN_NAME);
    }
    ColumnValueSelector[] reorderedDimensionSelectors = reorderedDimensionsWithTime
        .stream()
        .filter(column -> !ColumnHolder.TIME_COLUMN_NAME.equals(column))
        .map(dimName -> {
          int dimIndex = originalAdapter.getDimensionNames(false).indexOf(dimName);
          if (dimIndex >= 0) {
            return originalRowPointer.getDimensionSelector(dimIndex);
          } else {
            return NilColumnValueSelector.instance();
          }
        })
        .toArray(ColumnValueSelector[]::new);
    List<DimensionHandler> reorderedHandlers =
        reorderedDimensionsWithTime.stream()
                                   .filter(column -> !ColumnHolder.TIME_COLUMN_NAME.equals(column))
                                   .map(originalHandlers::get).collect(Collectors.toList());
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
          reorderedTimePosition,
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
          reorderedTimePosition,
          reorderedDimensionSelectors,
          reorderedHandlers,
          reorderedMetricSelectors,
          reorderedMetrics
      );
    }
  }

  static ColumnPartSerde createLongColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    return LongNumericColumnPartSerdeV2.serializerBuilder()
                                       .withByteOrder(IndexIO.BYTE_ORDER)
                                       .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                       .withDelegate(serializer)
                                       .build();
  }

  static ColumnPartSerde createDoubleColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    return DoubleNumericColumnPartSerdeV2.serializerBuilder()
                                         .withByteOrder(IndexIO.BYTE_ORDER)
                                         .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                         .withDelegate(serializer)
                                         .build();
  }

  static ColumnPartSerde createFloatColumnPartSerde(GenericColumnSerializer serializer, IndexSpec indexSpec)
  {
    return FloatNumericColumnPartSerdeV2.serializerBuilder()
                                        .withByteOrder(IndexIO.BYTE_ORDER)
                                        .withBitmapSerdeFactory(indexSpec.getBitmapSerdeFactory())
                                        .withDelegate(serializer)
                                        .build();
  }

  static GenericColumnSerializer createLongColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      IndexSpec indexSpec
  )
  {
    return LongColumnSerializerV2.create(
        columnName,
        segmentWriteOutMedium,
        columnName,
        indexSpec.getMetricCompression(),
        indexSpec.getLongEncoding(),
        indexSpec.getBitmapSerdeFactory()
    );
  }

  static GenericColumnSerializer createDoubleColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      IndexSpec indexSpec
  )
  {
    return DoubleColumnSerializerV2.create(
        columnName,
        segmentWriteOutMedium,
        columnName,
        indexSpec.getMetricCompression(),
        indexSpec.getBitmapSerdeFactory()
    );
  }

  static GenericColumnSerializer createFloatColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      IndexSpec indexSpec
  )
  {
    return FloatColumnSerializerV2.create(
        columnName,
        segmentWriteOutMedium,
        columnName,
        indexSpec.getMetricCompression(),
        indexSpec.getBitmapSerdeFactory()
    );
  }

  protected static class IndexMergeResult
  {
    @Nullable
    protected final List<IntBuffer> rowNumConversions;
    protected final int rowCount;

    private IndexMergeResult(@Nullable List<IntBuffer> rowNumConversions, int rowCount)
    {
      this.rowNumConversions = rowNumConversions;
      this.rowCount = rowCount;
    }
  }

  protected static class DimensionsSpecInspector
  {
    private final boolean storeEmptyColumns;
    private final Set<String> explicitDimensions;
    private final boolean includeAllDimensions;

    private DimensionsSpecInspector(
        boolean storeEmptyColumns,
        @Nullable DimensionsSpec dimensionsSpec
    )
    {
      this.storeEmptyColumns = storeEmptyColumns;
      this.explicitDimensions = dimensionsSpec == null
                                ? ImmutableSet.of()
                                : new HashSet<>(dimensionsSpec.getDimensionNames());
      this.includeAllDimensions = dimensionsSpec != null && (dimensionsSpec.isIncludeAllDimensions()
                                                             || dimensionsSpec.useSchemaDiscovery());
    }

    /**
     * Returns true if the given dimension should be stored in the segment even when the column has only nulls.
     * If it has non-nulls, then the column must be stored.
     *
     * @see DimensionMerger#hasOnlyNulls()
     */
    protected boolean shouldStore(String dimension)
    {
      return storeEmptyColumns && (includeAllDimensions || explicitDimensions.contains(dimension));
    }
  }
}
