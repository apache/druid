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
import com.google.common.primitives.Ints;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.TmpFileIOPeon;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(GenericIndexedBenchmark.ITERATIONS)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
@Fork(1)
@State(Scope.Benchmark)
public class GenericIndexedBenchmark
{
  public static final int ITERATIONS = 10000;

  static final ObjectStrategy<byte[]> byteArrayStrategy = new ObjectStrategy<byte[]>()
  {
    @Override
    public Class<? extends byte[]> getClazz()
    {
      return byte[].class;
    }

    @Override
    public byte[] fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      byte[] result = new byte[numBytes];
      buffer.get(result);
      return result;
    }

    @Override
    public byte[] toBytes(byte[] val)
    {
      return val;
    }

    @Override
    public int compare(byte[] o1, byte[] o2)
    {
      return Integer.compare(Ints.fromByteArray(o1), Ints.fromByteArray(o2));
    }
  };

  @Param({"10000"})
  public int n;
  @Param({"8"})
  public int elementSize;

  private File file;
  private File smooshDir;
  private GenericIndexed<byte[]> genericIndexed;
  private int[] iterationIndexes;
  private byte[][] elementsToSearch;

  @Setup(Level.Trial)
  public void createGenericIndexed() throws IOException
  {
    GenericIndexedWriter<byte[]> genericIndexedWriter = new GenericIndexedWriter<>(
        new TmpFileIOPeon(),
        "genericIndexedBenchmark",
        byteArrayStrategy
    );
    genericIndexedWriter.open();

    // GenericIndexObject caches prevObject for comparison, so need two arrays for correct objectsSorted computation.
    ByteBuffer[] elements = new ByteBuffer[2];
    elements[0] = ByteBuffer.allocate(elementSize);
    elements[1] = ByteBuffer.allocate(elementSize);
    for (int i = 0; i < n; i++) {
      ByteBuffer element = elements[i & 1];
      element.putInt(0, i);
      genericIndexedWriter.write(element.array());
    }
    genericIndexedWriter.close();
    smooshDir = Files.createTempDir();
    file = File.createTempFile("genericIndexedBenchmark", "meta");

    try (FileChannel fileChannel =
             FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
         FileSmoosher fileSmoosher = new FileSmoosher(smooshDir)) {
      genericIndexedWriter.writeToChannel(fileChannel, fileSmoosher);
    }

    FileChannel fileChannel = FileChannel.open(file.toPath());
    MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
    genericIndexed = GenericIndexed.read(byteBuffer, byteArrayStrategy, SmooshedFileMapper.load(smooshDir));
  }

  @Setup(Level.Trial)
  public void createIterationIndexes()
  {
    iterationIndexes = new int[ITERATIONS];
    for (int i = 0; i < ITERATIONS; i++) {
      iterationIndexes[i] = ThreadLocalRandom.current().nextInt(n);
    }
  }

  @Setup(Level.Trial)
  public void createElementsToSearch()
  {
    elementsToSearch = new byte[ITERATIONS][];
    for (int i = 0; i < ITERATIONS; i++) {
      elementsToSearch[i] = Ints.toByteArray(ThreadLocalRandom.current().nextInt(n));
    }
  }

  @Benchmark
  public void get(Blackhole bh)
  {
    for (int i : iterationIndexes) {
      bh.consume(genericIndexed.get(i));
    }
  }

  @Benchmark
  public int indexOf()
  {
    int r = 0;
    for (byte[] elementToSearch : elementsToSearch) {
      r ^= genericIndexed.indexOf(elementToSearch);
    }
    return r;
  }
}
