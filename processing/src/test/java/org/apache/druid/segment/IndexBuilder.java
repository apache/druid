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
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Helps tests make segments.
 */
public class IndexBuilder
{
  private static final int ROWS_PER_INDEX_FOR_MERGING = 1;
  private static final int DEFAULT_MAX_ROWS = Integer.MAX_VALUE;

  private IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
      .withMetrics(new CountAggregatorFactory("count"))
      .build();
  private SegmentWriteOutMediumFactory segmentWriteOutMediumFactory = OffHeapMemorySegmentWriteOutMediumFactory.instance();
  private IndexMerger indexMerger;
  private File tmpDir;
  private IndexSpec indexSpec = new IndexSpec();
  private int maxRows = DEFAULT_MAX_ROWS;

  private final ObjectMapper jsonMapper;
  private final IndexIO indexIO;
  private final List<InputRow> rows = new ArrayList<>();

  private IndexBuilder(ObjectMapper jsonMapper, ColumnConfig columnConfig)
  {
    this.jsonMapper = jsonMapper;
    this.indexIO = new IndexIO(jsonMapper, columnConfig);
    this.indexMerger = new IndexMergerV9(jsonMapper, indexIO, segmentWriteOutMediumFactory);
  }

  public static IndexBuilder create()
  {
    return new IndexBuilder(TestHelper.JSON_MAPPER, TestHelper.NO_CACHE_COLUMN_CONFIG);
  }

  public static IndexBuilder create(ColumnConfig columnConfig)
  {
    return new IndexBuilder(TestHelper.JSON_MAPPER, columnConfig);
  }

  public static IndexBuilder create(ObjectMapper jsonMapper)
  {
    return new IndexBuilder(jsonMapper, TestHelper.NO_CACHE_COLUMN_CONFIG);
  }

  public static IndexBuilder create(ObjectMapper jsonMapper, ColumnConfig columnConfig)
  {
    return new IndexBuilder(jsonMapper, columnConfig);
  }

  public IndexBuilder schema(IncrementalIndexSchema schema)
  {
    this.schema = schema;
    return this;
  }

  public IndexBuilder segmentWriteOutMediumFactory(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.indexMerger = new IndexMergerV9(jsonMapper, indexIO, segmentWriteOutMediumFactory);
    return this;
  }

  public IndexBuilder indexSpec(IndexSpec indexSpec)
  {
    this.indexSpec = indexSpec;
    return this;
  }

  public IndexBuilder tmpDir(File tmpDir)
  {
    this.tmpDir = tmpDir;
    return this;
  }

  public IndexBuilder rows(Iterable<InputRow> rows)
  {
    this.rows.clear();
    Iterables.addAll(this.rows, rows);
    return this;
  }

  public IncrementalIndex buildIncrementalIndex()
  {
    return buildIncrementalIndexWithRows(schema, maxRows, rows);
  }

  public QueryableIndex buildMMappedIndex()
  {
    Preconditions.checkNotNull(indexMerger, "indexMerger");
    Preconditions.checkNotNull(tmpDir, "tmpDir");
    try (final IncrementalIndex incrementalIndex = buildIncrementalIndex()) {
      return indexIO.loadIndex(
          indexMerger.persist(
              incrementalIndex,
              new File(
                  tmpDir,
                  StringUtils.format("testIndex-%s", ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE))
              ),
              indexSpec,
              null
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public QueryableIndex buildMMappedMergedIndex()
  {
    IndexMerger indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
    Preconditions.checkNotNull(tmpDir, "tmpDir");

    final List<QueryableIndex> persisted = new ArrayList<>();
    try {
      for (int i = 0; i < rows.size(); i += ROWS_PER_INDEX_FOR_MERGING) {
        persisted.add(
            TestHelper.getTestIndexIO().loadIndex(
                indexMerger.persist(
                    buildIncrementalIndexWithRows(
                        schema,
                        maxRows,
                        rows.subList(i, Math.min(rows.size(), i + ROWS_PER_INDEX_FOR_MERGING))
                    ),
                    new File(tmpDir, StringUtils.format("testIndex-%s", UUID.randomUUID().toString())),
                    indexSpec,
                    null
                )
            )
        );
      }
      final QueryableIndex merged = TestHelper.getTestIndexIO().loadIndex(
          indexMerger.mergeQueryableIndex(
              persisted,
              true,
              Iterables.toArray(
                  Iterables.transform(
                      Arrays.asList(schema.getMetrics()),
                      AggregatorFactory::getCombiningFactory
                  ),
                  AggregatorFactory.class
              ),
              null,
              new File(tmpDir, StringUtils.format("testIndex-%s", UUID.randomUUID())),
              indexSpec,
              indexSpec,
              new BaseProgressIndicator(),
              null,
              -1
          )
      );
      for (QueryableIndex index : persisted) {
        index.close();
      }
      return merged;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public RowBasedSegment<InputRow> buildRowBasedSegmentWithoutTypeSignature()
  {
    return new RowBasedSegment<>(
        SegmentId.dummy("IndexBuilder"),
        Sequences.simple(rows),
        RowAdapters.standardRow(),
        RowSignature.empty()
    );
  }

  public RowBasedSegment<InputRow> buildRowBasedSegmentWithTypeSignature()
  {
    // Determine row signature by building an mmapped index first.
    try (final QueryableIndex index = buildMMappedIndex()) {
      final RowSignature signature = new QueryableIndexStorageAdapter(index).getRowSignature();

      return new RowBasedSegment<>(
          SegmentId.dummy("IndexBuilder"),
          Sequences.simple(rows),
          RowAdapters.standardRow(),
          signature
      );
    }
  }

  public FrameSegment buildFrameSegment(FrameType frameType)
  {
    // Build mmapped index first, then copy over.
    try (final QueryableIndex index = buildMMappedIndex()) {
      return FrameTestUtil.adapterToFrameSegment(
          new QueryableIndexStorageAdapter(index),
          frameType,
          SegmentId.dummy("IndexBuilder")
      );
    }
  }

  private static IncrementalIndex buildIncrementalIndexWithRows(
      IncrementalIndexSchema schema,
      int maxRows,
      Iterable<InputRow> rows
  )
  {
    Preconditions.checkNotNull(schema, "schema");
    final IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRows)
        .build();

    for (InputRow row : rows) {
      try {
        incrementalIndex.add(row);
      }
      catch (IndexSizeExceededException e) {
        throw new RuntimeException(e);
      }
    }
    return incrementalIndex;
  }
}
