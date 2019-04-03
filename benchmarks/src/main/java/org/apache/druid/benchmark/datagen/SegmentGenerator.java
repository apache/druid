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

package org.apache.druid.benchmark.datagen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.hll.HyperLogLogHash;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexIndexableAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SegmentGenerator implements Closeable
{
  private static final Logger log = new Logger(SegmentGenerator.class);

  private static final int MAX_ROWS_IN_MEMORY = 200000;
  private static final int STARTING_SEED = 9999; // Consistent seed for reproducibility

  private final File tempDir;
  private final AtomicInteger seed;

  public SegmentGenerator()
  {
    this.tempDir = Files.createTempDir();
    this.seed = new AtomicInteger(STARTING_SEED);
  }

  public QueryableIndex generate(
      final DataSegment dataSegment,
      final BenchmarkSchemaInfo schemaInfo,
      final Granularity granularity,
      final int numRows
  )
  {
    // In case we need to generate hyperUniques.
    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }

    final BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        seed.getAndIncrement(),
        schemaInfo.getDataInterval(),
        numRows
    );

    final List<DimensionSchema> dimensions = new ArrayList<>();
    for (BenchmarkColumnSchema columnSchema : schemaInfo.getColumnSchemas()) {
      if (schemaInfo.getAggs().stream().noneMatch(agg -> agg.getName().equals(columnSchema.getName()))) {
        switch (columnSchema.getType()) {
          case STRING:
            dimensions.add(new StringDimensionSchema(columnSchema.getName()));
            break;
          case LONG:
            dimensions.add(new LongDimensionSchema(columnSchema.getName()));
            break;
          case DOUBLE:
            dimensions.add(new DoubleDimensionSchema(columnSchema.getName()));
            break;
          case FLOAT:
            dimensions.add(new FloatDimensionSchema(columnSchema.getName()));
            break;
          default:
            throw new ISE("Unhandleable type[%s]", columnSchema.getType());
        }
      }
    }

    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(dimensions, ImmutableList.of(), ImmutableList.of()))
        .withMetrics(schemaInfo.getAggsArray())
        .withRollup(schemaInfo.isWithRollup())
        .withQueryGranularity(granularity)
        .build();

    final List<InputRow> rows = new ArrayList<>();
    final List<QueryableIndex> indexes = new ArrayList<>();

    for (int i = 0; i < numRows; i++) {
      final InputRow row = dataGenerator.nextRow();
      rows.add(row);

      if ((i + 1) % 20000 == 0) {
        log.info("%,d/%,d rows generated.", i + 1, numRows);
      }

      if (rows.size() % MAX_ROWS_IN_MEMORY == 0) {
        indexes.add(makeIndex(dataSegment.getId(), indexes.size(), rows, indexSchema));
        rows.clear();
      }
    }

    log.info("%,d/%,d rows generated.", numRows, numRows);

    if (rows.size() > 0) {
      indexes.add(makeIndex(dataSegment.getId(), indexes.size(), rows, indexSchema));
      rows.clear();
    }

    if (indexes.isEmpty()) {
      throw new ISE("No rows to index?");
    } else if (indexes.size() == 1) {
      return Iterables.getOnlyElement(indexes);
    } else {
      try {
        final QueryableIndex merged = TestHelper.getTestIndexIO().loadIndex(
            TestHelper.getTestIndexMergerV9(OffHeapMemorySegmentWriteOutMediumFactory.instance()).merge(
                indexes.stream().map(QueryableIndexIndexableAdapter::new).collect(Collectors.toList()),
                false,
                schemaInfo.getAggs()
                          .stream()
                          .map(AggregatorFactory::getCombiningFactory)
                          .toArray(AggregatorFactory[]::new),
                new File(tempDir, "merged"),
                new IndexSpec()
            )
        );

        for (QueryableIndex index : indexes) {
          index.close();
        }

        return merged;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    FileUtils.deleteDirectory(tempDir);
  }

  private QueryableIndex makeIndex(
      final SegmentId identifier,
      final int indexNumber,
      final List<InputRow> rows,
      final IncrementalIndexSchema indexSchema
  )
  {
    return IndexBuilder
        .create()
        .schema(indexSchema)
        .tmpDir(new File(new File(tempDir, identifier.toString()), String.valueOf(indexNumber)))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .rows(rows)
        .buildMMappedIndex();
  }
}
