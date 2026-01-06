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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileBuilderV10;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.serde.NullColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link IndexMerger} for creating v10 format segments with {@link SegmentFileBuilderV10}
 *
 * @see SegmentFileBuilderV10
 * @see org.apache.druid.segment.file.SegmentFileMapperV10 to read the resulting segment file
 */
public class IndexMergerV10 extends IndexMergerBase
{
  private static final Logger log = new Logger(IndexMergerV10.class);

  public IndexMergerV10(
      ObjectMapper mapper,
      IndexIO indexIO,
      SegmentWriteOutMediumFactory defaultSegmentWriteOutMediumFactory
  )
  {
    super(mapper, indexIO, defaultSegmentWriteOutMediumFactory);
  }

  @Override
  protected boolean shouldStoreEmptyColumns()
  {
    return true;
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
    final String basePrefix = Projections.BASE_TABLE_PROJECTION_NAME + "/";

    progress.start();
    progress.progress();

    // Merged dimensions without __time.
    List<String> mergedDimensions =
        mergedDimensionsWithTime.stream()
                                .filter(dim -> !ColumnHolder.TIME_COLUMN_NAME.equals(dim))
                                .collect(Collectors.toList());
    Closer closer = Closer.create();
    try {
      final SegmentFileBuilderV10 v10Smoosher = new SegmentFileBuilderV10(
          mapper,
          IndexIO.V10_FILE_NAME,
          outDir,
          Integer.MAX_VALUE
      );

      DateTime minTime = DateTimes.MAX;
      DateTime maxTime = DateTimes.MIN;

      for (IndexableAdapter index : adapters) {
        minTime = JodaUtils.minDateTime(minTime, index.getDataInterval().getStart());
        maxTime = JodaUtils.maxDateTime(maxTime, index.getDataInterval().getEnd());
      }
      final Interval dataInterval = new Interval(minTime, maxTime);
      v10Smoosher.addInterval(dataInterval.toString());
      v10Smoosher.addBitmapEncoding(indexSpec.getBitmapSerdeFactory());

      FileUtils.mkdirp(outDir);

      SegmentWriteOutMediumFactory omf = segmentWriteOutMediumFactory != null
                                         ? segmentWriteOutMediumFactory
                                         : defaultSegmentWriteOutMediumFactory;
      log.debug("Using SegmentWriteOutMediumFactory[%s]", omf.getClass().getSimpleName());
      SegmentWriteOutMedium segmentWriteOutMedium = omf.makeSegmentWriteOutMedium(outDir);
      closer.register(segmentWriteOutMedium);
      long startTime = System.currentTimeMillis();

      SegmentizerFactory customSegmentLoader = indexSpec.getSegmentLoader();
      if (customSegmentLoader != null) {
        try (FileOutputStream fos = new FileOutputStream(new File(outDir, "factory.json"))) {
          mapper.writeValue(fos, customSegmentLoader);
          log.debug("Completed factory.json in %,d millis", System.currentTimeMillis() - startTime);
        }
      }

      progress.progress();
      // this whole section should also just be writing a projection for the base table using the same stuff, however,
      // the method that builds projections right now is missing the logic to mark mergers as 'parents' if any other
      // projections are relying on them (which keeps certain resources around longer so that they are still available
      // when merging the projection tables), and so it lives separately for now... will fix later
      final Map<String, ColumnFormat> metricFormats = new TreeMap<>(Comparators.naturalNullsFirst());
      final List<ColumnFormat> dimFormats = Lists.newArrayListWithCapacity(mergedDimensions.size());
      mergeFormat(adapters, mergedDimensions, metricFormats, dimFormats);

      final Map<String, DimensionHandler> handlers = makeDimensionHandlers(mergedDimensions, dimFormats);
      final Map<String, DimensionMergerV9> mergersMap = Maps.newHashMapWithExpectedSize(mergedDimensions.size());
      final List<DimensionMergerV9> mergers = new ArrayList<>();
      for (int i = 0; i < mergedDimensions.size(); i++) {
        DimensionHandler handler = handlers.get(mergedDimensions.get(i));
        DimensionMergerV9 merger = handler.makeMerger(
            basePrefix + mergedDimensions.get(i),
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

      // this part right here does the parent marking
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
          setupMetricsWriters(segmentWriteOutMedium, mergedMetrics, metricFormats, indexSpec, basePrefix);
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
      makeTimeColumn(v10Smoosher, progress, timeWriter, indexSpec, basePrefix + ColumnHolder.TIME_COLUMN_NAME);
      makeMetricsColumns(
          v10Smoosher,
          progress,
          mergedMetrics,
          metricFormats,
          metricWriters,
          indexSpec,
          basePrefix
      );

      for (int i = 0; i < mergedDimensions.size(); i++) {
        DimensionMergerV9 merger = mergers.get(i);
        merger.writeIndexes(indexMergeResult.rowNumConversions);
        if (!merger.hasOnlyNulls()) {
          ColumnDescriptor columnDesc = merger.makeColumnDescriptor();
          makeColumn(v10Smoosher, basePrefix + mergedDimensions.get(i), columnDesc);
        } else {
          ColumnDescriptor columnDesc = ColumnDescriptor
              .builder()
              .setValueType(dimFormats.get(i).getLogicalType().getType())
              .addSerde(new NullColumnPartSerde(indexMergeResult.rowCount, indexSpec.getBitmapSerdeFactory()))
              .build();
          makeColumn(v10Smoosher, basePrefix + mergedDimensions.get(i), columnDesc);
        }
      }

      progress.stopSection(section);

      // Recompute the projections.
      final Metadata finalMetadata;
      if (segmentMetadata == null || CollectionUtils.isNullOrEmpty(segmentMetadata.getProjections())) {
        finalMetadata = segmentMetadata;
      } else {
        finalMetadata = makeProjections(
            v10Smoosher,
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

      List<ProjectionMetadata> projections = new ArrayList<>();
      // ingestion current builds v9 metadata... translate v9 metadata and projection stuff to v10 format
      projections.add(
          ProjectionMetadata.forBaseTable(indexMergeResult.rowCount, mergedDimensionsWithTime, finalMetadata)
      );
      // convert v9 projections to v10 projections
      for (AggregateProjectionMetadata aggMeta : finalMetadata.getProjections()) {
        projections.add(new ProjectionMetadata(aggMeta.getNumRows(), aggMeta.getSchema()));
      }

      v10Smoosher.addProjections(projections);

      progress.progress();
      v10Smoosher.close();
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

  @Override
  protected void makeColumn(
      final SegmentFileBuilder segmentFileBuilder,
      final String columnName,
      final ColumnDescriptor serdeficator
  ) throws IOException
  {
    segmentFileBuilder.addColumn(columnName, serdeficator);
    try (SegmentFileChannel channel = segmentFileBuilder.addWithChannel(
        columnName,
        serdeficator.getSerializedSize()
    )) {
      serdeficator.writeTo(channel, segmentFileBuilder);
    }
  }
}
