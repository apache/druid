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

package io.druid.benchmark;

import com.google.common.io.Files;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.data.VSizeLongSerde;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VSizeSerdeBenchmark
{
  private static final Logger log = new Logger(VSizeSerdeBenchmark.class);
  @Param({"500000"})
  private int values;

  private VSizeLongSerde.LongDeserializer d1;
  private VSizeLongSerde.LongDeserializer d2;
  private VSizeLongSerde.LongDeserializer d4;
  private VSizeLongSerde.LongDeserializer d8;
  private VSizeLongSerde.LongDeserializer d12;
  private VSizeLongSerde.LongDeserializer d16;
  private VSizeLongSerde.LongDeserializer d20;
  private VSizeLongSerde.LongDeserializer d24;
  private VSizeLongSerde.LongDeserializer d32;
  private VSizeLongSerde.LongDeserializer d40;
  private VSizeLongSerde.LongDeserializer d48;
  private VSizeLongSerde.LongDeserializer d56;
  private VSizeLongSerde.LongDeserializer d64;
  private long sum;
  private File dummy;

  @Setup
  public void setup() throws IOException, URISyntaxException
  {
    // this uses a dummy file of sufficient size to construct a mappedByteBuffer instead of using ByteBuffer.allocate
    // to construct a heapByteBuffer since they have different performance
    File base = new File(this.getClass().getClassLoader().getResource("").toURI());
    dummy = new File(base, "dummy");
    try (Writer writer = java.nio.file.Files.newBufferedWriter(dummy.toPath(), StandardCharsets.UTF_8)) {
      String EMPTY_STRING = "        ";
      for (int i = 0; i < values + 10; i++) {
        writer.write(EMPTY_STRING);
      }
    }
    ByteBuffer buffer = Files.map(dummy);
    d1 = VSizeLongSerde.getDeserializer(1, buffer, 10);
    d2 = VSizeLongSerde.getDeserializer(2, buffer, 10);
    d4 = VSizeLongSerde.getDeserializer(4, buffer, 10);
    d8 = VSizeLongSerde.getDeserializer(8, buffer, 10);
    d12 = VSizeLongSerde.getDeserializer(12, buffer, 10);
    d16 = VSizeLongSerde.getDeserializer(16, buffer, 10);
    d20 = VSizeLongSerde.getDeserializer(20, buffer, 10);
    d24 = VSizeLongSerde.getDeserializer(24, buffer, 10);
    d32 = VSizeLongSerde.getDeserializer(32, buffer, 10);
    d40 = VSizeLongSerde.getDeserializer(40, buffer, 10);
    d48 = VSizeLongSerde.getDeserializer(48, buffer, 10);
    d56 = VSizeLongSerde.getDeserializer(56, buffer, 10);
    d64 = VSizeLongSerde.getDeserializer(64, buffer, 10);
  }

  @TearDown
  public void tearDown()
  {
    dummy.delete();
    log.info("%d", sum);
  }

  @Benchmark
  public void read1()
  {
    for (int i = 0; i < values; i++) {
      sum += d1.get(i);
    }
  }

  @Benchmark
  public void read2()
  {
    for (int i = 0; i < values; i++) {
      sum += d2.get(i);
    }
  }

  @Benchmark
  public void read4()
  {
    for (int i = 0; i < values; i++) {
      sum += d4.get(i);
    }
  }

  @Benchmark
  public void read8()
  {
    for (int i = 0; i < values; i++) {
      sum += d8.get(i);
    }
  }

  @Benchmark
  public void readd12()
  {
    for (int i = 0; i < values; i++) {
      sum += d12.get(i);
    }
  }

  @Benchmark
  public void readd16()
  {
    for (int i = 0; i < values; i++) {
      sum += d16.get(i);
    }
  }

  @Benchmark
  public void readd20()
  {
    for (int i = 0; i < values; i++) {
      sum += d20.get(i);
    }
  }

  @Benchmark
  public void readd24()
  {
    for (int i = 0; i < values; i++) {
      sum += d24.get(i);
    }
  }

  @Benchmark
  public void readd32()
  {
    for (int i = 0; i < values; i++) {
      sum += d32.get(i);
    }
  }

  @Benchmark
  public void readd40()
  {
    for (int i = 0; i < values; i++) {
      sum += d40.get(i);
    }
  }

  @Benchmark
  public void readd48()
  {
    for (int i = 0; i < values; i++) {
      sum += d48.get(i);
    }
  }

  @Benchmark
  public void readd56()
  {
    for (int i = 0; i < values; i++) {
      sum += d56.get(i);
    }
  }

  @Benchmark
  public void readd64()
  {
    for (int i = 0; i < values; i++) {
      sum += d64.get(i);
    }
  }
}
