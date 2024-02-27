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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Helps tests make segments.
 */
@SuppressWarnings({"NotNullFieldNotInitialized", "FieldMayBeFinal", "ConstantConditions", "NullableProblems"})
public class IndexBuilder
{
  private static final int ROWS_PER_INDEX_FOR_MERGING = 1;
  private static final int DEFAULT_MAX_ROWS = Integer.MAX_VALUE;

  private final ObjectMapper jsonMapper;
  private final IndexIO indexIO;
  private final List<InputRow> rows = new ArrayList<>();

  private SegmentWriteOutMediumFactory segmentWriteOutMediumFactory = OffHeapMemorySegmentWriteOutMediumFactory.instance();
  private IndexMerger indexMerger;
  private File tmpDir;
  private IndexSpec indexSpec = IndexSpec.DEFAULT;
  private int maxRows = DEFAULT_MAX_ROWS;
  private int intermediatePersistSize = ROWS_PER_INDEX_FOR_MERGING;
  private IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
      .withMetrics(new CountAggregatorFactory("count"))
      .build();
  @Nullable
  private InputSource inputSource = null;
  @Nullable
  private InputFormat inputFormat = null;
  @Nullable
  private TransformSpec transformSpec = null;
  @Nullable
  private File inputSourceTmpDir = null;

  private boolean writeNullColumns = false;

  private IndexBuilder(ObjectMapper jsonMapper, ColumnConfig columnConfig)
  {
    this.jsonMapper = jsonMapper;
    this.indexIO = new IndexIO(jsonMapper, columnConfig);
    this.indexMerger = new IndexMergerV9(jsonMapper, indexIO, segmentWriteOutMediumFactory);
  }

  public static IndexBuilder create()
  {
    return new IndexBuilder(TestHelper.JSON_MAPPER, ColumnConfig.ALWAYS_USE_INDEXES);
  }

  public static IndexBuilder create(ColumnConfig columnConfig)
  {
    return new IndexBuilder(TestHelper.JSON_MAPPER, columnConfig);
  }

  public static IndexBuilder create(ObjectMapper jsonMapper)
  {
    return new IndexBuilder(jsonMapper, ColumnConfig.ALWAYS_USE_INDEXES);
  }

  public static IndexBuilder create(ObjectMapper jsonMapper, ColumnConfig columnConfig)
  {
    return new IndexBuilder(jsonMapper, columnConfig);
  }

  public IndexIO getIndexIO()
  {
    return indexIO;
  }

  public IndexBuilder schema(IncrementalIndexSchema schema)
  {
    this.schema = schema;
    return this;
  }

  public IndexBuilder segmentWriteOutMediumFactory(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.indexMerger = new IndexMergerV9(jsonMapper, indexIO, segmentWriteOutMediumFactory, writeNullColumns);
    return this;
  }

  public IndexBuilder writeNullColumns(boolean shouldWriteNullColumns)
  {
    this.writeNullColumns = shouldWriteNullColumns;
    this.indexMerger = new IndexMergerV9(jsonMapper, indexIO, segmentWriteOutMediumFactory, shouldWriteNullColumns);
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

  public IndexBuilder inputSource(InputSource inputSource)
  {
    this.inputSource = inputSource;
    return this;
  }

  public IndexBuilder inputFormat(InputFormat inputFormat)
  {
    this.inputFormat = inputFormat;
    return this;
  }

  public IndexBuilder transform(TransformSpec transformSpec)
  {
    this.transformSpec = transformSpec;
    return this;
  }

  public IndexBuilder inputTmpDir(File inputSourceTmpDir)
  {
    this.inputSourceTmpDir = inputSourceTmpDir;
    return this;
  }

  public IndexBuilder rows(
      InputSource inputSource,
      InputFormat inputFormat,
      InputRowSchema rowSchema,
      TransformSpec transformSpec,
      File tmp
  )
      throws IOException
  {
    rows.clear();
    InputSourceReader reader = inputSource.reader(rowSchema, inputFormat, tmp);
    InputSourceReader transformingReader = transformSpec.decorate(reader);
    try (CloseableIterator<InputRow> rowIterator = transformingReader.read()) {
      while (rowIterator.hasNext()) {
        rows.add(rowIterator.next());
      }
    }
    return this;
  }

  public IndexBuilder rows(Iterable<InputRow> rows)
  {
    this.rows.clear();
    Iterables.addAll(this.rows, rows);
    return this;
  }

  public IndexBuilder intermediaryPersistSize(int rows)
  {
    this.intermediatePersistSize = rows;
    return this;
  }

  public IndexBuilder mapSchema(Function<IncrementalIndexSchema, IncrementalIndexSchema> f)
  {
    this.schema = f.apply(this.schema);
    return this;
  }

  public IncrementalIndex buildIncrementalIndex()
  {
    if (inputSource != null) {
      return buildIncrementalIndexWithInputSource(
          schema,
          inputSource,
          inputFormat,
          transformSpec,
          inputSourceTmpDir,
          maxRows
      );
    }
    return buildIncrementalIndexWithRows(schema, maxRows, rows);
  }

  public File buildMMappedIndexFile()
  {
    Preconditions.checkNotNull(indexMerger, "indexMerger");
    Preconditions.checkNotNull(tmpDir, "tmpDir");
    try (final IncrementalIndex incrementalIndex = buildIncrementalIndex()) {
      List<IndexableAdapter> adapters = Collections.singletonList(
          new QueryableIndexIndexableAdapter(
              indexIO.loadIndex(
                  indexMerger.persist(
                      incrementalIndex,
                      new File(
                          tmpDir,
                          StringUtils.format("testIndex-%s", ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE))
                      ),
                      indexSpec,
                      null
                  )
              )
          )
      );
      // Do a 'merge' of the persisted segment even though there is only one; this time it will be reading from the
      // queryable index instead of the incremental index, which also mimics the behavior of real ingestion tasks
      // which persist incremental indexes as intermediate segments and then merges all the intermediate segments to
      // publish
      return indexMerger.merge(
          adapters,
          schema.isRollup(),
          schema.getMetrics(),
          tmpDir,
          schema.getDimensionsSpec(),
          indexSpec,
          -1
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public QueryableIndex buildMMappedIndex()
  {
    try {
      return indexIO.loadIndex(buildMMappedIndexFile());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public QueryableIndex buildMMappedMergedIndex()
  {
    Preconditions.checkNotNull(tmpDir, "tmpDir");
    final List<QueryableIndex> persisted = new ArrayList<>();
    if (inputSource != null) {
      Preconditions.checkNotNull(inputSource, "inputSource");
      Preconditions.checkNotNull(inputFormat, "inputFormat");
      Preconditions.checkNotNull(inputSourceTmpDir, "inputSourceTmpDir");

      TransformSpec transformer = transformSpec != null ? transformSpec : TransformSpec.NONE;
      InputRowSchema rowSchema = new InputRowSchema(schema.getTimestampSpec(), schema.getDimensionsSpec(), null);
      InputSourceReader reader = inputSource.reader(rowSchema, inputFormat, inputSourceTmpDir);
      InputSourceReader transformingReader = transformer.decorate(reader);
      return mergeIndexes(indexMerger, persisted, transformingReader::read);
    }

    return mergeIndexes(indexMerger, persisted, () -> CloseableIterators.withEmptyBaggage(rows.iterator()));
  }

  @Nonnull
  private QueryableIndex mergeIndexes(
      IndexMerger indexMerger,
      List<QueryableIndex> persisted,
      IteratorSupplier iteratorSupplier
  )
  {
    try (CloseableIterator<InputRow> rowIterator = iteratorSupplier.get()) {
      int i = 0;
      IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
          .setIndexSchema(schema)
          .setMaxRowCount(maxRows)
          .build();
      while (rowIterator.hasNext()) {
        if (i < intermediatePersistSize) {
          incrementalIndex.add(rowIterator.next());
          i++;
        } else {
          persisted.add(
              indexIO.loadIndex(
                  indexMerger.persist(
                      incrementalIndex,
                      new File(tmpDir, StringUtils.format("testIndex-%s", UUID.randomUUID().toString())),
                      indexSpec,
                      null
                  )
              )
          );
          incrementalIndex = new OnheapIncrementalIndex.Builder()
              .setIndexSchema(schema)
              .setMaxRowCount(maxRows)
              .build();
          i = 0;
        }
      }
      if (i != 0) {
        persisted.add(
            indexIO.loadIndex(
                indexMerger.persist(
                    incrementalIndex,
                    new File(tmpDir, StringUtils.format("testIndex-%s", UUID.randomUUID().toString())),
                    indexSpec,
                    null
                )
            )
        );
      }

      final QueryableIndex merged = indexIO.loadIndex(
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
              schema.getDimensionsSpec(),
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

  private static IncrementalIndex buildIncrementalIndexWithInputSource(
      IncrementalIndexSchema schema,
      InputSource inputSource,
      InputFormat inputFormat,
      @Nullable TransformSpec transformSpec,
      File inputSourceTmpDir,
      int maxRows
  )
  {
    Preconditions.checkNotNull(schema, "schema");
    Preconditions.checkNotNull(inputSource, "inputSource");
    Preconditions.checkNotNull(inputFormat, "inputFormat");
    Preconditions.checkNotNull(inputSourceTmpDir, "inputSourceTmpDir");

    final IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRows)
        .build();
    TransformSpec tranformer = transformSpec != null ? transformSpec : TransformSpec.NONE;
    InputRowSchema rowSchema = new InputRowSchema(schema.getTimestampSpec(), schema.getDimensionsSpec(), null);
    InputSourceReader reader = inputSource.reader(rowSchema, inputFormat, inputSourceTmpDir);
    InputSourceReader transformingReader = tranformer.decorate(reader);
    try (CloseableIterator<InputRow> rowIterator = transformingReader.read()) {
      while (rowIterator.hasNext()) {
        incrementalIndex.add(rowIterator.next());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return incrementalIndex;
  }


  @FunctionalInterface
  interface IteratorSupplier
  {
    CloseableIterator<InputRow> get() throws IOException;
  }
}
