/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.datagen;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.hll.HyperLogLogHash;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.IndexBuilder;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

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
        indexes.add(makeIndex(dataSegment.getIdentifier(), indexes.size(), rows, indexSchema));
        rows.clear();
      }
    }

    log.info("%,d/%,d rows generated.", numRows, numRows);

    if (rows.size() > 0) {
      indexes.add(makeIndex(dataSegment.getIdentifier(), indexes.size(), rows, indexSchema));
      rows.clear();
    }

    if (indexes.isEmpty()) {
      throw new ISE("No rows to index?");
    } else if (indexes.size() == 1) {
      return Iterables.getOnlyElement(indexes);
    } else {
      try {
        final QueryableIndex merged = TestHelper.getTestIndexIO().loadIndex(
            TestHelper.getTestIndexMergerV9().merge(
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
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    FileUtils.deleteDirectory(tempDir);
  }

  private QueryableIndex makeIndex(
      final String identifier,
      final int indexNumber,
      final List<InputRow> rows,
      final IncrementalIndexSchema indexSchema
  )
  {
    return IndexBuilder
        .create()
        .schema(indexSchema)
        .tmpDir(new File(new File(tempDir, identifier), String.valueOf(indexNumber)))
        .indexMerger(TestHelper.getTestIndexMergerV9())
        .rows(rows)
        .buildMMappedIndex();
  }
}
