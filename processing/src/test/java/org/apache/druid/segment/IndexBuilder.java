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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;

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
  private IndexMerger indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
  private File tmpDir;
  private IndexSpec indexSpec = new IndexSpec();
  private int maxRows = DEFAULT_MAX_ROWS;

  private final List<InputRow> rows = new ArrayList<>();

  private IndexBuilder()
  {

  }

  public static IndexBuilder create()
  {
    return new IndexBuilder();
  }

  public IndexBuilder schema(IncrementalIndexSchema schema)
  {
    this.schema = schema;
    return this;
  }

  public IndexBuilder segmentWriteOutMediumFactory(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
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
      return TestHelper.getTestIndexIO().loadIndex(
          indexMerger.persist(
              incrementalIndex,
              new File(tmpDir, StringUtils.format("testIndex-%s", ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE))),
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
          indexMerger.merge(
              Lists.transform(
                  persisted,
                  new Function<QueryableIndex, IndexableAdapter>()
                  {
                    @Override
                    public IndexableAdapter apply(QueryableIndex input)
                    {
                      return new QueryableIndexIndexableAdapter(input);
                    }
                  }
              ),
              true,
              Iterables.toArray(
                  Iterables.transform(
                      Arrays.asList(schema.getMetrics()),
                      new Function<AggregatorFactory, AggregatorFactory>()
                      {
                        @Override
                        public AggregatorFactory apply(AggregatorFactory input)
                        {
                          return input.getCombiningFactory();
                        }
                      }
                  ),
                  AggregatorFactory.class
              ),
              new File(tmpDir, StringUtils.format("testIndex-%s", UUID.randomUUID())),
              indexSpec
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

  private static IncrementalIndex buildIncrementalIndexWithRows(
      IncrementalIndexSchema schema,
      int maxRows,
      Iterable<InputRow> rows
  )
  {
    Preconditions.checkNotNull(schema, "schema");
    final IncrementalIndex incrementalIndex = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRows)
        .buildOnheap();

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
