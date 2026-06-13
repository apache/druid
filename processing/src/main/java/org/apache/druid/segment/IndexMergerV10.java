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
import com.google.common.collect.Ordering;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileBuilderV10;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ClusteringDictionaries;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
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
    DruidException.conditionalDefensive(
        segmentMetadata != null,
        "Unexpected null Metadata when merging v10 segments"
    );

    // Clustered base tables take a different write path: rows live in per-cluster-group containers and the
    // top-level base table has no column files of its own. Detection has to look at the per-adapter metadata
    // because Metadata.merge intentionally leaves clusteredBaseTable null for the merged metadata (this method
    // computes the merged schema as part of the group merge).
    final List<ClusteredValueGroupsBaseTableSchema> clusterSchemas = new ArrayList<>(adapters.size());
    boolean anyClustered = false;
    boolean allClustered = true;
    for (IndexableAdapter adapter : adapters) {
      // Per-adapter metadata can be null (e.g. some test adapters); a null-metadata adapter is not clustered.
      final Metadata adapterMetadata = adapter.getMetadata();
      final ClusteredValueGroupsBaseTableSchema clusterSchema =
          adapterMetadata == null ? null : adapterMetadata.getClusteredBaseTable();
      clusterSchemas.add(clusterSchema);
      if (clusterSchema != null) {
        anyClustered = true;
      } else {
        allClustered = false;
      }
    }
    if (anyClustered) {
      DruidException.conditionalDefensive(
          allClustered,
          "Cannot merge clustered and non-clustered base table segments together"
      );
      DruidException.conditionalDefensive(
          CollectionUtils.isNullOrEmpty(segmentMetadata.getProjections()),
          "Clustered base table segments do not yet support aggregate projections"
      );
      return makeClusteredIndexFiles(
          adapters,
          clusterSchemas,
          segmentMetadata,
          outDir,
          progress,
          mergedMetrics,
          rowMergerFn,
          indexSpec,
          segmentWriteOutMediumFactory
      );
    }

    final String basePrefix = Projections.BASE_TABLE_PROJECTION_NAME + "/";

    progress.start();
    progress.progress();

    // Merged dimensions without __time.
    List<String> mergedDimensions =
        mergedDimensionsWithTime.stream()
                                .filter(dim -> !ColumnHolder.TIME_COLUMN_NAME.equals(dim))
                                .collect(Collectors.toList());
    Closer closer = Closer.create();

    final SegmentFileBuilderV10 v10Smoosher = SegmentFileBuilderV10.create(
        mapper,
        outDir,
        indexSpec.getMetadataCompression()
    );
    try {
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
      if (segmentMetadata.getProjections() != null) {
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
      final IndexMergeResult indexMergeResult = mergeIndexesAndWriteColumns(
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
      v10Smoosher.startFileBundle(Projections.BASE_TABLE_PROJECTION_NAME);
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

      final List<ProjectionMetadata> projections = new ArrayList<>();
      // ingestion current builds v9 metadata... translate v9 metadata and projection stuff to v10 format
      projections.add(
          ProjectionMetadata.forBaseTable(
              indexMergeResult.rowCount,
              indexMergeResult.minTime,
              indexMergeResult.maxTime,
              mergedDimensionsWithTime,
              segmentMetadata
          )
      );

      // make the projections
      if (!CollectionUtils.isNullOrEmpty(segmentMetadata.getProjections())) {
        final Metadata updatedMetadata = makeProjections(
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
        // convert v9 projections to v10 projections (projections cannot be null if we got here)
        for (AggregateProjectionMetadata aggMeta : updatedMetadata.getProjections()) {
          projections.add(new ProjectionMetadata(aggMeta.getNumRows(), aggMeta.getSchema()));
        }
      }

      v10Smoosher.addProjections(projections);

      progress.progress();
      v10Smoosher.close();
      progress.stop();

      return outDir;
    }
    catch (Throwable t) {
      CloseableUtils.closeAndWrapExceptions(v10Smoosher);
      throw closer.rethrow(t);
    }
    finally {
      closer.close();
    }
  }

  /**
   * Write a clustered base-table segment: every input adapter is a clustered base table whose rows live in
   * per-clustering-tuple cluster groups. Groups are aligned across adapters by clustering tuple (after remapping
   * each input's dictionary IDs into the merged segment-wide dictionaries), then each merged group is merged like
   * a miniature base table, its own dimension mergers, dictionaries, and column files are written under the
   * group's {@code __base$<id0>_<id1>.../} prefix in its own file bundle. The top-level base table itself has no
   * column files; its {@link ProjectionMetadata} entry carries the merged
   * {@link ClusteredValueGroupsBaseTableSchema} that readers use to navigate the per-group bundles.
   */
  private File makeClusteredIndexFiles(
      final List<IndexableAdapter> adapters,
      final List<ClusteredValueGroupsBaseTableSchema> clusterSchemas,
      final Metadata segmentMetadata,
      final File outDir,
      final ProgressIndicator progress,
      final List<String> mergedMetrics,
      final Function<List<TransformableRowIterator>, TimeAndDimsIterator> rowMergerFn,
      final IndexSpec indexSpec,
      final @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    progress.start();
    progress.progress();

    final Closer closer = Closer.create();
    final SegmentFileBuilderV10 v10Smoosher = SegmentFileBuilderV10.create(
        mapper,
        outDir,
        indexSpec.getMetadataCompression()
    );
    try {
      DateTime minDateTime = DateTimes.MAX;
      DateTime maxDateTime = DateTimes.MIN;
      for (IndexableAdapter index : adapters) {
        minDateTime = JodaUtils.minDateTime(minDateTime, index.getDataInterval().getStart());
        maxDateTime = JodaUtils.maxDateTime(maxDateTime, index.getDataInterval().getEnd());
      }
      final Interval dataInterval = new Interval(minDateTime, maxDateTime);
      v10Smoosher.addInterval(dataInterval.toString());
      v10Smoosher.addBitmapEncoding(indexSpec.getBitmapSerdeFactory());

      FileUtils.mkdirp(outDir);

      final SegmentWriteOutMediumFactory omf = segmentWriteOutMediumFactory != null
                                               ? segmentWriteOutMediumFactory
                                               : defaultSegmentWriteOutMediumFactory;
      log.debug("Using SegmentWriteOutMediumFactory[%s]", omf.getClass().getSimpleName());
      final SegmentWriteOutMedium segmentWriteOutMedium = omf.makeSegmentWriteOutMedium(outDir);
      closer.register(segmentWriteOutMedium);

      final SegmentizerFactory customSegmentLoader = indexSpec.getSegmentLoader();
      if (customSegmentLoader != null) {
        try (FileOutputStream fos = new FileOutputStream(new File(outDir, "factory.json"))) {
          mapper.writeValue(fos, customSegmentLoader);
        }
      }

      progress.progress();
      final MergedClusterGroups merged = mergeClusterSchemas(clusterSchemas);

      // Merge each cluster group as a miniature base table. Groups are visited in merged-tuple order (the TreeMap
      // is sorted lexicographically by remapped dictionary IDs = clustering value order), which is the order the
      // read side expects them in.
      final List<TableClusterGroupSpec> mergedGroupSpecs = new ArrayList<>(merged.groupSources.size());
      final LinkedHashSet<String> allGroupDims = new LinkedHashSet<>();
      int totalRows = 0;
      Long minTime = null;
      Long maxTime = null;

      for (Map.Entry<List<Integer>, List<AdapterAndGroup>> groupEntry : merged.groupSources.entrySet()) {
        final List<Integer> mergedTuple = groupEntry.getKey();
        final String groupPrefix = Projections.getClusterGroupSegmentInternalFilePrefix(mergedTuple);
        // file-bundle name is the prefix without the trailing slash
        final String groupName = groupPrefix.substring(0, groupPrefix.length() - 1);

        final List<IndexableAdapter> groupAdapters = Lists.newArrayListWithCapacity(groupEntry.getValue().size());
        for (AdapterAndGroup source : groupEntry.getValue()) {
          groupAdapters.add(adapters.get(source.adapterIndex).getClusterGroupAdapter(source.groupSpec));
        }

        // Per-group dimensions: derived from this group's adapters (not segment-wide), mirroring the projection
        // merge path. Columns missing from a group are simply absent from its bundle; the read side treats them
        // as nil columns.
        final List<String> groupDimsWithTime = IndexMerger.getMergedDimensionsWithTime(groupAdapters, null);
        final List<String> groupDims = groupDimsWithTime.stream()
                                                        .filter(d -> !ColumnHolder.TIME_COLUMN_NAME.equals(d))
                                                        .collect(Collectors.toList());
        allGroupDims.addAll(groupDims);

        final Map<String, ColumnFormat> metricFormats = new TreeMap<>(Comparators.naturalNullsFirst());
        final List<ColumnFormat> dimFormats = Lists.newArrayListWithCapacity(groupDims.size());
        mergeFormat(groupAdapters, groupDims, metricFormats, dimFormats);

        final Map<String, DimensionHandler> handlers = makeDimensionHandlers(groupDims, dimFormats);
        final List<DimensionMergerV9> mergers = new ArrayList<>(groupDims.size());
        for (int i = 0; i < groupDims.size(); i++) {
          final DimensionHandler handler = handlers.get(groupDims.get(i));
          mergers.add(
              handler.makeMerger(
                  groupPrefix + groupDims.get(i),
                  indexSpec,
                  segmentWriteOutMedium,
                  dimFormats.get(i).toColumnCapabilities(),
                  progress,
                  outDir,
                  closer
              )
          );
        }

        progress.progress();
        writeDimValuesAndSetupDimConversion(groupAdapters, progress, groupDims, mergers);

        progress.progress();
        final TimeAndDimsIterator timeAndDimsIterator = makeMergedTimeAndDimsIterator(
            groupAdapters,
            groupDimsWithTime,
            mergedMetrics,
            rowMergerFn,
            handlers,
            mergers
        );
        closer.register(timeAndDimsIterator);
        final GenericColumnSerializer timeWriter = setupTimeWriter(segmentWriteOutMedium, indexSpec);
        final ArrayList<GenericColumnSerializer> metricWriters =
            setupMetricsWriters(segmentWriteOutMedium, mergedMetrics, metricFormats, indexSpec, groupPrefix);
        final IndexMergeResult groupMergeResult = mergeIndexesAndWriteColumns(
            groupAdapters,
            progress,
            timeAndDimsIterator,
            timeWriter,
            metricWriters,
            mergers
        );

        final String section = "build cluster group[" + groupName + "] inverted index and columns";
        progress.startSection(section);
        v10Smoosher.startFileBundle(groupName);
        makeTimeColumn(v10Smoosher, progress, timeWriter, indexSpec, groupPrefix + ColumnHolder.TIME_COLUMN_NAME);
        makeMetricsColumns(
            v10Smoosher,
            progress,
            mergedMetrics,
            metricFormats,
            metricWriters,
            indexSpec,
            groupPrefix
        );
        for (int i = 0; i < groupDims.size(); i++) {
          final DimensionMergerV9 merger = mergers.get(i);
          merger.writeIndexes(groupMergeResult.rowNumConversions);
          final ColumnDescriptor columnDesc;
          if (merger.hasOnlyNulls()) {
            columnDesc = ColumnDescriptor
                .builder()
                .setValueType(dimFormats.get(i).getLogicalType().getType())
                .addSerde(new NullColumnPartSerde(groupMergeResult.rowCount, indexSpec.getBitmapSerdeFactory()))
                .build();
          } else {
            columnDesc = merger.makeColumnDescriptor();
          }
          makeColumn(v10Smoosher, groupPrefix + groupDims.get(i), columnDesc);
        }
        progress.stopSection(section);

        mergedGroupSpecs.add(new TableClusterGroupSpec(mergedTuple, groupMergeResult.rowCount));
        totalRows += groupMergeResult.rowCount;
        if (groupMergeResult.minTime != null) {
          minTime = minTime == null ? groupMergeResult.minTime : Math.min(minTime, groupMergeResult.minTime);
        }
        if (groupMergeResult.maxTime != null) {
          maxTime = maxTime == null ? groupMergeResult.maxTime : Math.max(maxTime, groupMergeResult.maxTime);
        }
      }

      // Assemble the merged summary schema. Aggregators use the merged (combining) form from the segment metadata,
      // consistent with how the non-clustered base table metadata is built.
      final ClusteredValueGroupsBaseTableSchema firstSchema = clusterSchemas.get(0);
      final List<String> summaryColumns = new ArrayList<>();
      summaryColumns.addAll(firstSchema.getClusteringColumns().getColumnNames());
      summaryColumns.add(ColumnHolder.TIME_COLUMN_NAME);
      for (String dim : allGroupDims) {
        if (!ColumnHolder.TIME_COLUMN_NAME.equals(dim)) {
          summaryColumns.add(dim);
        }
      }
      final ClusteredValueGroupsBaseTableSchema mergedSchema = new ClusteredValueGroupsBaseTableSchema(
          firstSchema.getVirtualColumns(),
          summaryColumns,
          segmentMetadata.getAggregators(),
          firstSchema.getOrdering(),
          firstSchema.getClusteringColumns(),
          null,
          merged.dictionaries,
          mergedGroupSpecs
      );

      v10Smoosher.addProjections(
          List.of(new ProjectionMetadata(totalRows, mergedSchema, minTime, maxTime))
      );

      progress.progress();
      v10Smoosher.close();
      progress.stop();

      return outDir;
    }
    catch (Throwable t) {
      CloseableUtils.closeAndWrapExceptions(v10Smoosher);
      throw closer.rethrow(t);
    }
    finally {
      closer.close();
    }
  }

  /**
   * Merge the per-input cluster schemas: validate compatibility, union the per-type clustering dictionaries
   * (sorted, nulls first), and align groups across inputs by their clustering tuple after remapping each input's
   * dictionary IDs into the merged dictionaries. Tuples sort lexicographically by merged ID, which equals
   * clustering-value order (nulls first) because the merged dictionaries are sorted.
   */
  private static MergedClusterGroups mergeClusterSchemas(List<ClusteredValueGroupsBaseTableSchema> schemas)
  {
    final ClusteredValueGroupsBaseTableSchema first = schemas.get(0);
    final RowSignature clusteringColumns = first.getClusteringColumns();
    for (ClusteredValueGroupsBaseTableSchema schema : schemas) {
      DruidException.conditionalDefensive(
          clusteringColumns.equals(schema.getClusteringColumns()),
          "Cannot merge clustered segments with different clustering columns: [%s] vs [%s]",
          clusteringColumns,
          schema.getClusteringColumns()
      );
      DruidException.conditionalDefensive(
          first.getOrdering().equals(schema.getOrdering()),
          "Cannot merge clustered segments with different ordering: [%s] vs [%s]",
          first.getOrdering(),
          schema.getOrdering()
      );
      DruidException.conditionalDefensive(
          Objects.equals(first.getVirtualColumns(), schema.getVirtualColumns()),
          "Cannot merge clustered segments with different clustering virtual columns: [%s] vs [%s]",
          first.getVirtualColumns(),
          schema.getVirtualColumns()
      );
      // Per-group metric columns are written for the segment-wide merged metric set; the per-group merge derives its
      // metric formats from only that group's source adapters. All adapters reaching here descend from a single
      // IncrementalIndexSchema (intermediary persists of one ingest/compaction job), so every cluster group carries
      // every metric and the two sets coincide. Assert that invariant defensively: a future path that merges
      // arbitrary clustered segments with differing aggregators would otherwise NPE when writing a group missing a
      // segment-wide metric.
      DruidException.conditionalDefensive(
          Arrays.equals(first.getAggregators(), schema.getAggregators()),
          "Cannot merge clustered segments with different aggregators: [%s] vs [%s]",
          Arrays.toString(first.getAggregators()),
          Arrays.toString(schema.getAggregators())
      );
    }

    // Union per-type dictionaries, sorted with nulls first, plus per-input value -> merged id lookups.
    final MergedDictionary<String> strings = mergeDictionary(
        schemas,
        s -> s.getClusteringDictionaries().getStringDictionary()
    );
    final MergedDictionary<Long> longs = mergeDictionary(
        schemas,
        s -> s.getClusteringDictionaries().getLongDictionary()
    );
    final MergedDictionary<Double> doubles = mergeDictionary(
        schemas,
        s -> s.getClusteringDictionaries().getDoubleDictionary()
    );
    final MergedDictionary<Float> floats = mergeDictionary(
        schemas,
        s -> s.getClusteringDictionaries().getFloatDictionary()
    );

    final ClusteringDictionaries mergedDictionaries = new ClusteringDictionaries(
        strings.sorted,
        longs.sorted,
        doubles.sorted,
        floats.sorted
    );

    // Align groups across inputs by remapped tuple, in lexicographic (= clustering value) order.
    final TreeMap<List<Integer>, List<AdapterAndGroup>> groupSources = new TreeMap<>(TUPLE_COMPARATOR);
    for (int adapterIndex = 0; adapterIndex < schemas.size(); adapterIndex++) {
      final ClusteredValueGroupsBaseTableSchema schema = schemas.get(adapterIndex);
      for (TableClusterGroupSpec spec : schema.getClusterGroups()) {
        final List<Integer> oldIds = spec.getClusteringValueIds();
        final List<Integer> newIds = new ArrayList<>(oldIds.size());
        for (int i = 0; i < oldIds.size(); i++) {
          final int position = i;
          final ColumnType type = clusteringColumns.getColumnType(i).orElseThrow(
              () -> DruidException.defensive("clustering column at position [%s] has no type", position)
          );
          final Object value = schema.getClusteringDictionaries().lookupValue(type, oldIds.get(i));
          final int newId = lookupMergedId(type, value, strings, longs, doubles, floats);
          newIds.add(newId);
        }
        groupSources.computeIfAbsent(newIds, ids -> new ArrayList<>())
                    .add(new AdapterAndGroup(adapterIndex, spec));
      }
    }

    return new MergedClusterGroups(mergedDictionaries, groupSources);
  }

  private static <T extends Comparable<T>> MergedDictionary<T> mergeDictionary(
      List<ClusteredValueGroupsBaseTableSchema> schemas,
      Function<ClusteredValueGroupsBaseTableSchema, List<T>> dictExtractor
  )
  {
    boolean hasNull = false;
    final TreeSet<T> values = new TreeSet<>();
    for (ClusteredValueGroupsBaseTableSchema schema : schemas) {
      for (T value : dictExtractor.apply(schema)) {
        if (value == null) {
          hasNull = true;
        } else {
          values.add(value);
        }
      }
    }
    if (!hasNull && values.isEmpty()) {
      return new MergedDictionary<>(List.of(), Map.of(), -1);
    }
    final List<T> sorted = new ArrayList<>(values.size() + (hasNull ? 1 : 0));
    final Map<T, Integer> idLookup = new HashMap<>();
    int nullId = -1;
    if (hasNull) {
      nullId = 0;
      sorted.add(null);
    }
    for (T value : values) {
      idLookup.put(value, sorted.size());
      sorted.add(value);
    }
    return new MergedDictionary<>(sorted, idLookup, nullId);
  }

  private static int lookupMergedId(
      ColumnType type,
      @Nullable Object value,
      MergedDictionary<String> strings,
      MergedDictionary<Long> longs,
      MergedDictionary<Double> doubles,
      MergedDictionary<Float> floats
  )
  {
    final MergedDictionary<?> dict;
    if (type.is(ValueType.STRING)) {
      dict = strings;
    } else if (type.is(ValueType.LONG)) {
      dict = longs;
    } else if (type.is(ValueType.DOUBLE)) {
      dict = doubles;
    } else if (type.is(ValueType.FLOAT)) {
      dict = floats;
    } else {
      throw DruidException.defensive("unsupported clustering type [%s]", type);
    }
    if (value == null) {
      DruidException.conditionalDefensive(dict.nullId >= 0, "merged dictionary missing null entry");
      return dict.nullId;
    }
    final Integer id = dict.idLookup.get(value);
    if (id == null) {
      throw DruidException.defensive("merged dictionary missing clustering value [%s]", value);
    }
    return id;
  }

  /**
   * Orders clustering-ID tuples by clustering-value order: {@code natural()} compares IDs numerically (not as
   * strings), lifted position-by-position via {@code lexicographical()}. Numeric ID order tracks value order because
   * the merged per-type dictionaries are sorted (nulls-first) before IDs are assigned.
   */
  private static final Comparator<Iterable<Integer>> TUPLE_COMPARATOR = Ordering.<Integer>natural().lexicographical();

  private static final class MergedDictionary<T>
  {
    private final List<T> sorted;
    private final Map<T, Integer> idLookup;
    private final int nullId;

    private MergedDictionary(List<T> sorted, Map<T, Integer> idLookup, int nullId)
    {
      this.sorted = sorted;
      this.idLookup = idLookup;
      this.nullId = nullId;
    }
  }

  private static final class AdapterAndGroup
  {
    private final int adapterIndex;
    private final TableClusterGroupSpec groupSpec;

    private AdapterAndGroup(int adapterIndex, TableClusterGroupSpec groupSpec)
    {
      this.adapterIndex = adapterIndex;
      this.groupSpec = groupSpec;
    }
  }

  private static final class MergedClusterGroups
  {
    private final ClusteringDictionaries dictionaries;
    private final TreeMap<List<Integer>, List<AdapterAndGroup>> groupSources;

    private MergedClusterGroups(
        ClusteringDictionaries dictionaries,
        TreeMap<List<Integer>, List<AdapterAndGroup>> groupSources
    )
    {
      this.dictionaries = dictionaries;
      this.groupSources = groupSources;
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
