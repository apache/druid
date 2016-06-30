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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.CompressionUtils;
import com.metamx.common.io.smoosh.Smoosh;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriterV1Factory;
import io.druid.segment.data.Indexed;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BufferIndexedBenchmark
{
  Indexed<String> aColumnV1;
  Indexed<String> aColumnV2;

  @Setup
  public void setup() throws IOException
  {
    // This segment was originally generated using the wikipedia realtime example
    final InputStream zippedSegmentStream = Preconditions.checkNotNull(
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("index.zip"),
        "Cannot find segment to benchmark against"
    );
    File tempDir = Files.createTempDir();
    File v1SegmentFile = new File(tempDir.getAbsolutePath() + "v1");
    if (!v1SegmentFile.mkdir()) {
      throw new IOException("Cannot create tmp dir for unzipping v1 segment");
    }

    Preconditions.checkState(CompressionUtils.unzip(zippedSegmentStream, v1SegmentFile).getFiles().size() == 3);

    File v2SegmentFile = new File(tempDir.getAbsolutePath() + "v2");

    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("added", "added"),
        new DoubleSumAggregatorFactory("deleted", "deleted"),
        new DoubleSumAggregatorFactory("delta", "delta")
    };
    convertV1toV2(
        v1SegmentFile,
        v2SegmentFile,
        aggregatorFactories,
        new IndexSpec(null, null, null)
    );


    // Read a v1 and v2 segment that will be used for benchmarking
    SmooshedFileMapper smooshedFilesV1 = Smoosh.map(v1SegmentFile);
    SmooshedFileMapper smooshedFilesV2 = Smoosh.map(v2SegmentFile);
    ByteBuffer indexBuffer1 = smooshedFilesV1.mapFile("index.drd");
    ByteBuffer indexBuffer2 = smooshedFilesV2.mapFile("index.drd");
    /**
     * index.drd should consist of the segment version, the columns and dimensions of the segment as generic
     * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
     */
    GenericIndexed.read(indexBuffer1, GenericIndexed.STRING_STRATEGY); // skip columns
    final GenericIndexed<String> dims1 = GenericIndexed.read(indexBuffer1, GenericIndexed.STRING_STRATEGY);
    GenericIndexed.read(indexBuffer2, GenericIndexed.STRING_STRATEGY); // skip columns
    final GenericIndexed<String> dims2 = GenericIndexed.read(indexBuffer2, GenericIndexed.STRING_STRATEGY);

    // 11th dimension is "page" and will have relatively high cardinality
    ByteBuffer columnBuffer1 = smooshedFilesV1.mapFile(dims1.get(11));
    ByteBuffer columnBuffer2 = smooshedFilesV2.mapFile(dims2.get(11));

    final int lengthToSkip1 = columnBuffer1.getInt(); //skip ColumnDescriptor
    final int lengthToSkip2 = columnBuffer2.getInt(); //skip ColumnDescriptor
    byte[] b1 = new byte[lengthToSkip1];
    byte[] b2 = new byte[lengthToSkip2];
    columnBuffer1.get(b1);
    columnBuffer2.get(b2);

    columnBuffer1.get(); // skip compressed version
    columnBuffer1.getInt(); // skip feature flag
    columnBuffer2.get(); // skip compressed version
    columnBuffer2.getInt(); // skip feature flag
    aColumnV1 = GenericIndexed.read(columnBuffer1, GenericIndexed.STRING_STRATEGY);
    aColumnV2 = GenericIndexed.read(columnBuffer2, GenericIndexed.STRING_STRATEGY);
  }

  private static void convertV1toV2(
      File inputFile,
      File outputFile,
      AggregatorFactory[] aggregatorFactories,
      IndexSpec indexSpec
  ) throws IOException
  {

    ColumnConfig columnConfig = new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return "processing-%s";
      }
    };
    ObjectMapper objectMapper = new DefaultObjectMapper();
    IndexIO indexIO = new IndexIO(objectMapper, columnConfig, new GenericIndexedWriterV1Factory());
    QueryableIndex queryableIndex = indexIO.loadIndex(inputFile);
    IndexMerger indexMerger = new IndexMerger(objectMapper, indexIO, new GenericIndexedWriterV1Factory());
    indexMerger.mergeQueryableIndex(
        Lists.newArrayList(queryableIndex),
        aggregatorFactories,
        outputFile,
        indexSpec
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void benchmarkV1(Blackhole blackhole)
  {
    for (int i = 0; i < aColumnV1.size(); i++) {
      blackhole.consume(aColumnV1.get(i));
      blackhole.consume(aColumnV1.indexOf(aColumnV1.get(i)));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void benchmarkV2(Blackhole blackhole)
  {
    for (int i = 0; i < aColumnV2.size(); i++) {
      blackhole.consume(aColumnV2.get(i));
      blackhole.consume(aColumnV2.indexOf(aColumnV2.get(i)));
    }
  }

  /**
   * For running the benchmarks directly using IDE
   */
  public static void main(String[] args) throws IOException
  {
    Options opt = new OptionsBuilder()
        .include(BufferIndexedBenchmark.class.getSimpleName())
        .warmupIterations(10)
        .measurementIterations(10)
        .mode(Mode.AverageTime)
        .timeUnit(TimeUnit.MICROSECONDS)
        .forks(1)
        .build();

    try {
      new Runner(opt).run();
    }
    catch (RunnerException e) {
      e.printStackTrace();
    }
  }
}
