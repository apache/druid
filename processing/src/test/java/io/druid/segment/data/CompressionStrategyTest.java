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

package io.druid.segment.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class CompressionStrategyTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> compressionStrategies()
  {
    return Iterables.transform(
        Arrays.asList(CompressedObjectStrategy.CompressionStrategy.values()),
        new Function<CompressedObjectStrategy.CompressionStrategy, Object[]>()
        {
          @Override
          public Object[] apply(CompressedObjectStrategy.CompressionStrategy compressionStrategy)
          {
            return new Object[]{compressionStrategy};
          }
        }
    );
  }

  protected final CompressedObjectStrategy.CompressionStrategy compressionStrategy;

  public CompressionStrategyTest(CompressedObjectStrategy.CompressionStrategy compressionStrategy)
  {
    this.compressionStrategy = compressionStrategy;
  }

  // MUST be smaller than CompressedPools.BUFFER_SIZE
  private static final int DATA_SIZER = 0xFFFF;
  private static byte[] originalData;

  @BeforeClass
  public static void setupClass()
  {
    originalData = new byte[DATA_SIZER];
    Random random = new Random(54671457);
    random.nextBytes(originalData);
  }

  @Test
  public void testBasicOperations()
  {
    ByteBuffer compressed = ByteBuffer.wrap(compressionStrategy.getCompressor().compress(originalData));
    ByteBuffer output = ByteBuffer.allocate(originalData.length);
    compressionStrategy.getDecompressor().decompress(compressed, compressed.array().length, output);
    byte[] checkArray = new byte[DATA_SIZER];
    output.get(checkArray);
    Assert.assertArrayEquals("Uncompressed data does not match", originalData, checkArray);
  }


  @Test
  public void testOutputSizeKnownOperations()
  {
    ByteBuffer compressed = ByteBuffer.wrap(compressionStrategy.getCompressor().compress(originalData));
    ByteBuffer output = ByteBuffer.allocate(originalData.length);
    compressionStrategy.getDecompressor()
                       .decompress(compressed, compressed.array().length, output, originalData.length);
    byte[] checkArray = new byte[DATA_SIZER];
    output.get(checkArray);
    Assert.assertArrayEquals("Uncompressed data does not match", originalData, checkArray);
  }

  @Test
  public void testDirectMemoryOperations()
  {
    ByteBuffer compressed = ByteBuffer.wrap(compressionStrategy.getCompressor().compress(originalData));
    ByteBuffer output = ByteBuffer.allocateDirect(originalData.length);
    compressionStrategy.getDecompressor().decompress(compressed, compressed.array().length, output);
    byte[] checkArray = new byte[DATA_SIZER];
    output.get(checkArray);
    Assert.assertArrayEquals("Uncompressed data does not match", originalData, checkArray);
  }

  @Test(timeout = 60000)
  public void testConcurrency() throws Exception
  {
    final int numThreads = 20;
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(numThreads);
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
        numThreads,
        numThreads,
        100,
        TimeUnit.MILLISECONDS,
        queue
    );
    Collection<Future<Boolean>> results = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i) {
      results.add(
          threadPoolExecutor.submit(
              new Callable<Boolean>()
              {
                @Override
                public Boolean call() throws Exception
                {
                  ByteBuffer compressed = ByteBuffer.wrap(compressionStrategy.getCompressor().compress(originalData));
                  ByteBuffer output = ByteBuffer.allocate(originalData.length);
                  compressionStrategy.getDecompressor().decompress(compressed, compressed.array().length, output);
                  byte[] checkArray = new byte[DATA_SIZER];
                  output.get(checkArray);
                  Assert.assertArrayEquals("Uncompressed data does not match", originalData, checkArray);
                  return true;
                }
              }
          )
      );
    }
    threadPoolExecutor.shutdown();
    for (Future result : results) {
      Assert.assertTrue((Boolean) result.get());
    }
  }


  @Test(timeout = 60000)
  public void testKnownSizeConcurrency() throws Exception
  {
    final int numThreads = 20;

    ListeningExecutorService threadPoolExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads));
    List<ListenableFuture<?>> results = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i) {
      results.add(
          threadPoolExecutor.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  ByteBuffer compressed = ByteBuffer.wrap(compressionStrategy.getCompressor().compress(originalData));
                  ByteBuffer output = ByteBuffer.allocate(originalData.length);
                  // TODO: Lambdas would be nice here whenever we use Java 8
                  compressionStrategy.getDecompressor()
                                     .decompress(compressed, compressed.array().length, output, originalData.length);
                  byte[] checkArray = new byte[DATA_SIZER];
                  output.get(checkArray);
                  Assert.assertArrayEquals("Uncompressed data does not match", originalData, checkArray);
                }
              }
          )
      );
    }
    Futures.allAsList(results).get();
  }
}
