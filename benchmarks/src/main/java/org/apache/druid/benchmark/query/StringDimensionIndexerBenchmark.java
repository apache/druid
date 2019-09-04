package org.apache.druid.benchmark.query;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.StringDimensionIndexer;
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

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class StringDimensionIndexerBenchmark
{
  StringDimensionIndexer indexer;
  int[] exampleArray;

  @Param({"10000"})
  public int cardinality;

  @Param({"8"})
  public int rowSize;

  @Setup
  public void setup()
  {
    indexer = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), true);

    for (int i = 0; i < cardinality; i++) {
      indexer.processRowValsToUnsortedEncodedKeyComponent("abcd-" + i, true);
    }

    exampleArray = new int[rowSize];
    int stride = cardinality / rowSize;
    for (int i = 0; i < rowSize; i++) {
      exampleArray[i] = i * stride;
    }

    System.out.println(indexer.getMaxValue());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void estimateEncodedKeyComponentSize(Blackhole blackhole)
  {
    long sz = indexer.estimateEncodedKeyComponentSize(exampleArray);
    blackhole.consume(sz);
  }
}
