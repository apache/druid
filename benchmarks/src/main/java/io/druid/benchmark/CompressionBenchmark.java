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

import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.LongSupplierSerializer;
import io.druid.segment.data.TmpFileIOPeon;
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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CompressionBenchmark
{
  @Param({"reqInt", "bytesLong", "timestamp"})
  private static String file;

  @Param({"delta", "longs"})
  private static String format;

  @Param({"lz4", "uncompressed", "none"})
  private static String strategy;

  private File compFile;
  private ByteBuffer buffer;
  private Random rand;
  private IndexedLongs indexedLongs;
  private int count;
  private long sum;

  @Setup
  public void setup() throws Exception
  {
    buildDruid();
    rand = new Random();
    buffer = Files.map(compFile);
    indexedLongs = CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder()).get();
    count = indexedLongs.size();
    System.out.println("count : " + count);
  }

  @TearDown
  public void teardown()
  {
    System.out.println("sum : " + sum);
  }

  private void buildDruid() throws IOException, URISyntaxException
  {
    URL url = this.getClass().getClassLoader().getResource(file + ".gz");
    File inFile = new File(url.toURI());
    compFile = new File(inFile.getParent(), file + "." + format + "-" + strategy);
    CompressionFactory.LongEncoding encodingFormat = CompressionFactory.LongEncoding.valueOf(format.toUpperCase());
    CompressedObjectStrategy.CompressionStrategy compressionStrategy = CompressedObjectStrategy.CompressionStrategy.valueOf(strategy.toUpperCase());
    compFile.delete();
    TmpFileIOPeon iopeon = new TmpFileIOPeon(true);
    LongSupplierSerializer writer = CompressionFactory.getLongSerializer(
        iopeon,
        "long",
        ByteOrder.nativeOrder(),
        encodingFormat,
        compressionStrategy
    );
    GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(inFile));
    BufferedReader br = new BufferedReader(new InputStreamReader(gzip));

    try (FileChannel output = FileChannel.open(
        compFile.toPath(),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
    )) {
      writer.open();
      String line;
      while ((line = br.readLine()) != null) {
        writer.add(Long.parseLong(line));
      }
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      writer.closeAndConsolidate(
          new ByteSink()
          {
            @Override
            public OutputStream openStream() throws IOException
            {
              return baos;
            }
          }
      );
      output.write(ByteBuffer.wrap(baos.toByteArray()));
    }
    finally {
      iopeon.cleanup();
      br.close();
    }
  }

  @Benchmark
  public void readContinuous()
  {
    sum = 0;
    for (int i = 0; i < count; i++) {
      sum += indexedLongs.get(i);
    }
  }

  @Benchmark
  public void readSkipping()
  {
    sum = 0;
    for (int i = 0; i < count; i += rand.nextInt(10000)) {
      sum += indexedLongs.get(i);
    }
  }

}

