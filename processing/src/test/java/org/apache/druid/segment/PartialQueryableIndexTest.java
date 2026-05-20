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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

class PartialQueryableIndexTest extends InitializedNullHandlingTest
{
  private static final ColumnConfig COLUMN_CONFIG = ColumnConfig.DEFAULT;
  private static final DateTime TIME = DateTimes.of("2025-01-01");

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("dim2", ColumnType.STRING)
                                                                .add("metric1", ColumnType.LONG)
                                                                .build();

  private static final List<AggregateProjectionSpec> PROJECTIONS = Collections.singletonList(
      AggregateProjectionSpec.builder("dim1_hourly_metric1_sum")
                             .virtualColumns(
                                 Granularities.toVirtualColumn(
                                     Granularities.HOUR,
                                     Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                 )
                             )
                             .groupingColumns(
                                 new StringDimensionSchema("dim1"),
                                 new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
                             )
                             .aggregators(
                                 new LongSumAggregatorFactory("_metric1_sum", "metric1"),
                                 new CountAggregatorFactory("_count")
                             )
                             .build()
  );

  private static final List<InputRow> ROWS = Arrays.asList(
      new ListBasedInputRow(ROW_SIGNATURE, TIME, ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", "x", 1L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(1), ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", "y", 2L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(2), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", "x", 3L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(3), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", "y", 4L))
  );

  @TempDir
  static File sharedTempDir;

  // the built V10 segment directory, shared across tests since it's read-only
  private static File segmentDir;

  @BeforeAll
  static void buildSegment()
  {
    final File tmpDir = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    segmentDir = IndexBuilder.create()
                             .useV10()
                             .tmpDir(tmpDir)
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(
                                 IncrementalIndexSchema.builder()
                                                      .withDimensionsSpec(
                                                          DimensionsSpec.builder()
                                                                        .setDimensions(
                                                                            List.of(
                                                                                new StringDimensionSchema("dim1"),
                                                                                new StringDimensionSchema("dim2"),
                                                                                new LongDimensionSchema("metric1")
                                                                            )
                                                                        )
                                                                        .build()
                                                      )
                                                      .withRollup(false)
                                                      .withMinTimestamp(TIME.getMillis())
                                                      .withProjections(PROJECTIONS)
                                                      .build()
                             )
                             .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.ZSTD).build())
                             .rows(ROWS)
                             .buildMMappedIndexFile();
  }

  @Test
  void testSchemaWithoutDownloads() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final File cacheDir = newCacheDir("schema");

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList()
    )) {
      rangeReader.resetCount();

      final PartialQueryableIndex index = new PartialQueryableIndex(
          mapper.getSegmentFileMetadata(),
          mapper,
          COLUMN_CONFIG
      );

      // all these should work without triggering any range reads (downloads)
      Assertions.assertNotNull(index.getDataInterval());
      Assertions.assertEquals(4, index.getNumRows());
      Assertions.assertNotNull(index.getAvailableDimensions());
      Assertions.assertNotNull(index.getMetadata());
      Assertions.assertNotNull(index.getOrdering());
      Assertions.assertFalse(index.getColumnNames().isEmpty());
      Assertions.assertNotNull(index.getBitmapFactoryForDimensions());

      // no downloads triggered
      Assertions.assertEquals(0, rangeReader.getReadCount());
      Assertions.assertEquals(Set.of(), rangeReader.getReadFilenames());
    }
  }

  @Test
  void testGetColumnCapabilitiesFromMetadata() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final File cacheDir = newCacheDir("caps");

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList()
    )) {
      rangeReader.resetCount();

      final PartialQueryableIndex index = new PartialQueryableIndex(
          mapper.getSegmentFileMetadata(),
          mapper,
          COLUMN_CONFIG
      );

      // string dimension
      ColumnCapabilities dim1Caps = index.getColumnCapabilities("dim1");
      Assertions.assertNotNull(dim1Caps);
      Assertions.assertEquals(ValueType.STRING, dim1Caps.getType());

      // long metric
      ColumnCapabilities metric1Caps = index.getColumnCapabilities("metric1");
      Assertions.assertNotNull(metric1Caps);
      Assertions.assertEquals(ValueType.LONG, metric1Caps.getType());

      // time column
      ColumnCapabilities timeCaps = index.getColumnCapabilities(ColumnHolder.TIME_COLUMN_NAME);
      Assertions.assertNotNull(timeCaps);

      // non-existent column
      Assertions.assertNull(index.getColumnCapabilities("nonexistent"));

      // no downloads triggered
      Assertions.assertEquals(0, rangeReader.getReadCount());
      Assertions.assertEquals(Set.of(), rangeReader.getReadFilenames());
    }
  }

  @Test
  void testGetColumnHolderTriggersBaseTableLoad() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final File cacheDir = newCacheDir("colholder");

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList()
    )) {
      rangeReader.resetCount();

      final PartialQueryableIndex index = new PartialQueryableIndex(
          mapper.getSegmentFileMetadata(),
          mapper,
          COLUMN_CONFIG
      );

      // no downloads yet
      Assertions.assertEquals(0, rangeReader.getReadCount());

      // accessing a column holder should trigger downloads
      Assertions.assertNotNull(index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME));
      Assertions.assertTrue(rangeReader.getReadCount() > 0);

      Assertions.assertNotNull(index.getColumnHolder("dim1"));
      Assertions.assertNull(index.getColumnHolder("nonexistent"));

      // all reads went to the V10 main segment file (no externals queried)
      Assertions.assertEquals(Set.of(IndexIO.V10_FILE_NAME), rangeReader.getReadFilenames());
    }
  }

  @Test
  void testGetProjectionMatchesFromMetadataAndLoadsLazily() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final File cacheDir = newCacheDir("projection");

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList()
    )) {
      final PartialQueryableIndex index = new PartialQueryableIndex(
          mapper.getSegmentFileMetadata(),
          mapper,
          COLUMN_CONFIG
      );

      // build a CursorBuildSpec that should match the projection
      final CursorBuildSpec matchingSpec = CursorBuildSpec.builder()
          .setInterval(index.getDataInterval())
          .setPhysicalColumns(Set.of("dim1", "metric1"))
          .setGroupingColumns(Collections.singletonList("dim1"))
          .setVirtualColumns(
              VirtualColumns.create(
                  Granularities.toVirtualColumn(Granularities.HOUR, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
              )
          )
          .setAggregators(
              List.of(
                  new LongSumAggregatorFactory("_metric1_sum", "metric1"),
                  new CountAggregatorFactory("_count")
              )
          )
          .build();

      rangeReader.resetCount();

      final QueryableProjection<QueryableIndex> projection = index.getProjection(matchingSpec);
      Assertions.assertNotNull(projection, "projection should match");

      // matching the projection itself shouldn't trigger any downloads, it's metadata-based
      Assertions.assertEquals(0, rangeReader.getReadCount(), "matching should not download files");
      Assertions.assertEquals(Set.of(), rangeReader.getReadFilenames(), "matching should not download files");
      Assertions.assertEquals(Set.of(), mapper.getDownloadedFiles(), "matching should not download files");

      final QueryableIndex projIndex = projection.getRowSelector();
      Assertions.assertNotNull(projIndex);
      Assertions.assertEquals(0, rangeReader.getReadCount(), "this should not download files either");
      // actually accessing a column on the projection triggers the column's download
      Assertions.assertNotNull(projIndex.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME));
      Assertions.assertTrue(rangeReader.getReadCount() > 0, "accessing a projection column should download");
      Assertions.assertEquals(Set.of(IndexIO.V10_FILE_NAME), rangeReader.getReadFilenames());

      // downloaded files are scoped to the matched projection's namespace, not the base table (if no shared parts)
      final Set<String> downloaded = mapper.getDownloadedFiles();
      Assertions.assertTrue(
          downloaded.stream().anyMatch(name -> name.startsWith("dim1_hourly_metric1_sum/")),
          "expected at least one file from the matched projection's namespace, got " + downloaded
      );
      Assertions.assertTrue(
          downloaded.stream().noneMatch(name -> name.startsWith("__base/")),
          "no base table files should be downloaded when only the projection was accessed, got " + downloaded
      );

      // fetching a projection column which has a base table parent does download base table stuff
      Assertions.assertNotNull(projIndex.getColumnHolder("dim1"));
      final Set<String> downloadedAfterDim1 = mapper.getDownloadedFiles();
      Assertions.assertTrue(
          downloadedAfterDim1.stream().anyMatch(name -> name.startsWith("dim1_hourly_metric1_sum/")),
          "expected at least one file from the matched projection's namespace, got " + downloadedAfterDim1
      );
      Assertions.assertTrue(
          downloadedAfterDim1.stream().anyMatch(name -> name.startsWith("__base/")),
          "base table files should be downloaded when a projection column shares data with a base table parent, got " + downloadedAfterDim1
      );
    }
  }

  @Test
  void testPerColumnLaziness() throws IOException
  {
    // verify that accessing one column of a projection doesn't download other columns
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final File cacheDir = newCacheDir("per_col");

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList()
    )) {
      final PartialQueryableIndex index = new PartialQueryableIndex(
          mapper.getSegmentFileMetadata(),
          mapper,
          COLUMN_CONFIG
      );

      rangeReader.resetCount();

      // access one base table column
      Assertions.assertNotNull(index.getColumnHolder("dim1"));
      final int countAfterDim1 = rangeReader.getReadCount();
      Assertions.assertTrue(countAfterDim1 > 0, "accessing dim1 should trigger downloads");

      // dim1's smoosh entry is downloaded; metric1's is not
      final Set<String> filesAfterDim1 = mapper.getDownloadedFiles();
      Assertions.assertTrue(filesAfterDim1.contains("__base/dim1"), "expected __base/dim1 in " + filesAfterDim1);
      Assertions.assertFalse(filesAfterDim1.contains("__base/metric1"), "metric1 should not be downloaded yet");

      // access the same column again should not trigger more downloads
      Assertions.assertNotNull(index.getColumnHolder("dim1"));
      Assertions.assertEquals(countAfterDim1, rangeReader.getReadCount(), "re-access should be cached");
      Assertions.assertEquals(filesAfterDim1, mapper.getDownloadedFiles(), "re-access should not download new files");

      // access a different column should trigger additional downloads for its files
      Assertions.assertNotNull(index.getColumnHolder("metric1"));
      Assertions.assertTrue(
          rangeReader.getReadCount() > countAfterDim1,
          "accessing metric1 should trigger additional downloads"
      );

      // metric1's smoosh entry is now also downloaded
      final Set<String> filesAfterMetric1 = mapper.getDownloadedFiles();
      Assertions.assertTrue(filesAfterMetric1.contains("__base/dim1"));
      Assertions.assertTrue(filesAfterMetric1.contains("__base/metric1"), "expected __base/metric1 in " + filesAfterMetric1);

      // all reads went to the V10 main segment file (no externals queried)
      Assertions.assertEquals(Set.of(IndexIO.V10_FILE_NAME), rangeReader.getReadFilenames());
    }
  }

  @Test
  void testGetProjectionReturnsNullForNonAggregateQuery() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final File cacheDir = newCacheDir("no_proj");

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList()
    )) {
      final PartialQueryableIndex index = new PartialQueryableIndex(
          mapper.getSegmentFileMetadata(),
          mapper,
          COLUMN_CONFIG
      );

      // scan query, no grouping, no aggregation, should not match any projection
      final CursorBuildSpec scanSpec = CursorBuildSpec.builder()
          .setInterval(index.getDataInterval())
          .build();

      Assertions.assertNull(index.getProjection(scanSpec));
    }
  }

  @Test
  void testMatchesEagerQueryableIndex() throws IOException
  {
    // verify that the partial index produces the same schema info as the eager (full) index
    final IndexIO indexIO = TestHelper.getTestIndexIO();
    final File cacheDir = newCacheDir("match_eager");
    final DirectoryRangeReader rangeReader = new DirectoryRangeReader(segmentDir);

    try (
        QueryableIndex eagerIndex = indexIO.loadIndex(segmentDir);
        PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
            rangeReader,
            TestHelper.makeJsonMapper(),
            cacheDir,
            IndexIO.V10_FILE_NAME,
            Collections.emptyList()
        )
    ) {
      final PartialQueryableIndex partialIndex = new PartialQueryableIndex(
          mapper.getSegmentFileMetadata(),
          mapper,
          COLUMN_CONFIG
      );

      Assertions.assertEquals(eagerIndex.getDataInterval(), partialIndex.getDataInterval());
      Assertions.assertEquals(eagerIndex.getNumRows(), partialIndex.getNumRows());
      final List<String> eagerDims = new ArrayList<>();
      eagerIndex.getAvailableDimensions().forEach(eagerDims::add);
      final List<String> partialDims = new ArrayList<>();
      partialIndex.getAvailableDimensions().forEach(partialDims::add);
      Assertions.assertEquals(eagerDims, partialDims);
      Assertions.assertEquals(eagerIndex.getColumnNames(), partialIndex.getColumnNames());
      Assertions.assertEquals(eagerIndex.getOrdering(), partialIndex.getOrdering());

      // verify column capabilities match for all columns
      for (String colName : eagerIndex.getColumnNames()) {
        final ColumnCapabilities eagerCaps = eagerIndex.getColumnCapabilities(colName);
        final ColumnCapabilities partialCaps = partialIndex.getColumnCapabilities(colName);
        Assertions.assertNotNull(eagerCaps, "eager caps for " + colName);
        Assertions.assertNotNull(partialCaps, "partial caps for " + colName);
        Assertions.assertEquals(
            eagerCaps.toColumnType(),
            partialCaps.toColumnType(),
            "type mismatch for " + colName
        );
      }
    }
  }

  private File newCacheDir(String name) throws IOException
  {
    final File dir = new File(sharedTempDir, name + "_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(dir);
    return dir;
  }

  static class DirectoryRangeReader implements SegmentRangeReader
  {
    private final File directory;

    DirectoryRangeReader(File directory)
    {
      this.directory = directory;
    }

    @Override
    public InputStream readRange(String filename, long offset, long length) throws IOException
    {
      File target = new File(directory, filename);
      try (RandomAccessFile raf = new RandomAccessFile(target, "r")) {
        final int available = (int) Math.min(length, Math.max(0, raf.length() - offset));
        byte[] data = new byte[available];
        raf.seek(offset);
        raf.readFully(data);
        return new ByteArrayInputStream(data);
      }
    }
  }

  static class CountingRangeReader extends DirectoryRangeReader
  {
    private final AtomicInteger readCount = new AtomicInteger(0);
    private final Set<String> readFilenames = ConcurrentHashMap.newKeySet();

    CountingRangeReader(File directory)
    {
      super(directory);
    }

    int getReadCount()
    {
      return readCount.get();
    }

    Set<String> getReadFilenames()
    {
      return Set.copyOf(readFilenames);
    }

    void resetCount()
    {
      readCount.set(0);
      readFilenames.clear();
    }

    @Override
    public InputStream readRange(String filename, long offset, long length) throws IOException
    {
      readCount.incrementAndGet();
      readFilenames.add(filename);
      return super.readRange(filename, offset, length);
    }
  }
}
