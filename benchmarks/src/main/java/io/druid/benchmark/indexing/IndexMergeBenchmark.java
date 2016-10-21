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

package io.druid.benchmark.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = "-server", value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IndexMergeBenchmark
{
  @Param({"5"})
  private int numSegments;

  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  private static final Logger log = new Logger(IndexMergeBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMerger INDEX_MERGER;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private List<QueryableIndex> indexesToMerge;
  private BenchmarkSchemaInfo schemaInfo;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER = new IndexMerger(JSON_MAPPER, INDEX_IO);
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + + System.currentTimeMillis());

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }

    indexesToMerge = new ArrayList<>();

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    for (int i = 0; i < numSegments; i++) {
      BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
          schemaInfo.getColumnSchemas(),
          RNG_SEED + i,
          schemaInfo.getDataInterval(),
          rowsPerSegment
      );

      IncrementalIndex incIndex = makeIncIndex();

      for (int j = 0; j < rowsPerSegment; j++) {
        InputRow row = gen.nextRow();
        if (j % 10000 == 0) {
          log.info(j + " rows generated.");
        }
        incIndex.add(row);
      }

      File tmpFile = Files.createTempDir();
      log.info("Using temp dir: " + tmpFile.getAbsolutePath());
      tmpFile.deleteOnExit();

      File indexFile = INDEX_MERGER_V9.persist(
          incIndex,
          tmpFile,
          new IndexSpec()
      );

      QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      indexesToMerge.add(qIndex);
    }
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder()
            .withQueryGranularity(QueryGranularities.NONE)
            .withMetrics(schemaInfo.getAggsArray())
            .withDimensionsSpec(new DimensionsSpec(null, null, null))
            .withRollup(rollup)
            .build(),
        true,
        false,
        true,
        rowsPerSegment
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void merge(Blackhole blackhole) throws Exception
  {
    File tmpFile = File.createTempFile("IndexMergeBenchmark-MERGEDFILE-" + System.currentTimeMillis(), ".TEMPFILE");
    tmpFile.delete();
    tmpFile.mkdirs();
    log.info(tmpFile.getAbsolutePath() + " isFile: " + tmpFile.isFile() + " isDir:" + tmpFile.isDirectory());
    tmpFile.deleteOnExit();

    File mergedFile = INDEX_MERGER.mergeQueryableIndex(indexesToMerge, rollup, schemaInfo.getAggsArray(), tmpFile, new IndexSpec());

    blackhole.consume(mergedFile);

    tmpFile.delete();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void mergeV9(Blackhole blackhole) throws Exception
  {
    File tmpFile = File.createTempFile("IndexMergeBenchmark-MERGEDFILE-V9-" + System.currentTimeMillis(), ".TEMPFILE");
    tmpFile.delete();
    tmpFile.mkdirs();
    log.info(tmpFile.getAbsolutePath() + " isFile: " + tmpFile.isFile() + " isDir:" + tmpFile.isDirectory());
    tmpFile.deleteOnExit();

    File mergedFile = INDEX_MERGER_V9.mergeQueryableIndex(indexesToMerge, rollup, schemaInfo.getAggsArray(), tmpFile, new IndexSpec());

    blackhole.consume(mergedFile);

    tmpFile.delete();
  }
}
