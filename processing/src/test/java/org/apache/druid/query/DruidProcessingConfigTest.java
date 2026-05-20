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

package org.apache.druid.query;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 *
 */
public class DruidProcessingConfigTest
{
  private static final long BUFFER_SIZE = 1024L * 1024L * 1024L;
  private static final int NUM_PROCESSORS = 4;
  private static final long DIRECT_SIZE = BUFFER_SIZE * (3L + 2L + 1L);
  private static final long HEAP_SIZE = BUFFER_SIZE * 2L;

  @Test
  public void testDefaultsMultiProcessor()
  {
    Injector injector = makeInjector(NUM_PROCESSORS, DIRECT_SIZE, HEAP_SIZE);
    DruidProcessingConfig config = injector.getInstance(DruidProcessingConfig.class);

    Assertions.assertEquals(Integer.MAX_VALUE, config.poolCacheMaxCount());
    Assertions.assertEquals(NUM_PROCESSORS - 1, config.getNumThreads());
    Assertions.assertEquals(Math.max(2, config.getNumThreads() / 4), config.getNumMergeBuffers());
    Assertions.assertTrue(config.isFifo());
    Assertions.assertEquals(System.getProperty("java.io.tmpdir"), config.getTmpDir());
    Assertions.assertEquals(BUFFER_SIZE, config.intermediateComputeSizeBytes());
  }

  @Test
  public void testDefaultsSingleProcessor()
  {
    Injector injector = makeInjector(1, BUFFER_SIZE * 4L, HEAP_SIZE);
    DruidProcessingConfig config = injector.getInstance(DruidProcessingConfig.class);

    Assertions.assertEquals(Integer.MAX_VALUE, config.poolCacheMaxCount());
    Assertions.assertTrue(config.getNumThreads() == 1);
    Assertions.assertEquals(Math.max(2, config.getNumThreads() / 4), config.getNumMergeBuffers());
    Assertions.assertTrue(config.isFifo());
    Assertions.assertEquals(System.getProperty("java.io.tmpdir"), config.getTmpDir());
    Assertions.assertEquals(BUFFER_SIZE, config.intermediateComputeSizeBytes());
  }

  @Test
  public void testDefaultsLargeDirect()
  {
    // test that auto sized buffer is no larger than 1
    Injector injector = makeInjector(1, BUFFER_SIZE * 100L, HEAP_SIZE);
    DruidProcessingConfig config = injector.getInstance(DruidProcessingConfig.class);

    Assertions.assertEquals(
        DruidProcessingBufferConfig.MAX_DEFAULT_PROCESSING_BUFFER_SIZE_BYTES,
        config.intermediateComputeSizeBytes()
    );
  }

  @Test
  public void testReplacements()
  {
    Properties props = new Properties();
    props.setProperty("druid.processing.buffer.sizeBytes", "1");
    props.setProperty("druid.processing.buffer.poolCacheMaxCount", "1");
    props.setProperty("druid.processing.numThreads", "256");
    props.setProperty("druid.processing.fifo", "false");
    props.setProperty("druid.processing.tmpDir", "/test/path");


    Injector injector = makeInjector(
        NUM_PROCESSORS,
        DIRECT_SIZE,
        HEAP_SIZE,
        props
    );
    DruidProcessingConfig config = injector.getInstance(DruidProcessingConfig.class);

    Assertions.assertEquals(1, config.intermediateComputeSizeBytes()); // heh
    Assertions.assertEquals(1, config.poolCacheMaxCount());
    Assertions.assertEquals(256, config.getNumThreads());
    Assertions.assertEquals(64, config.getNumMergeBuffers());
    Assertions.assertFalse(config.isFifo());
    Assertions.assertEquals("/test/path", config.getTmpDir());
    Assertions.assertEquals(0, config.getNumInitalBuffersForIntermediatePool());
  }

  @Test
  public void testParallelPoolInitDefaultIsFalse()
  {
    Injector injector = makeInjector(NUM_PROCESSORS, DIRECT_SIZE, HEAP_SIZE);
    DruidProcessingConfig config = injector.getInstance(DruidProcessingConfig.class);
    Assertions.assertFalse(config.isParallelMemoryPoolInit());
  }

  @Test
  public void testParallelPoolInitEnabled()
  {
    Properties props = new Properties();
    props.setProperty("druid.processing.parallelPoolInit", "true");
    Injector injector = makeInjector(NUM_PROCESSORS, DIRECT_SIZE, HEAP_SIZE, props);
    DruidProcessingConfig config = injector.getInstance(DruidProcessingConfig.class);
    Assertions.assertTrue(config.isParallelMemoryPoolInit());
  }

  @Test
  public void testInvalidSizeBytes()
  {
    Properties props = new Properties();
    props.setProperty("druid.processing.buffer.sizeBytes", "-1");

    Injector injector = makeInjector(
        NUM_PROCESSORS,
        DIRECT_SIZE,
        HEAP_SIZE,
        props
    );

    ExceptionMatcher
        .of(ProvisionException.class)
        .expectMessageContains(
            "Cannot construct instance of `HumanReadableBytes`, problem: Invalid format of number: -1. Negative value is not allowed."
        )
        .assertThrowsAndMatches(() -> injector.getInstance(DruidProcessingConfig.class));
  }

  @Test
  public void testSizeBytesUpperLimit()
  {
    Properties props = new Properties();
    props.setProperty("druid.processing.buffer.sizeBytes", "2GiB");
    Injector injector = makeInjector(
        NUM_PROCESSORS,
        DIRECT_SIZE,
        HEAP_SIZE,
        props
    );
    Throwable t = Assertions.assertThrows(
        ProvisionException.class,
        () -> injector.getInstance(DruidProcessingConfig.class)
    );

    Assertions.assertTrue(t.getMessage().contains("druid.processing.buffer.sizeBytes must be less than 2GiB"));
  }

  private static Injector makeInjector(int numProcessors, long directMemorySize, long heapSize)
  {
    return makeInjector(numProcessors, directMemorySize, heapSize, new Properties());
  }

  private static Injector makeInjector(
      int numProcessors,
      long directMemorySize,
      long heapSize,
      Properties props
  )
  {
    final Injector startupInjector = new StartupInjectorBuilder().withProperties(props).build();
    return Guice.createInjector(
        startupInjector.getInstance(DruidSecondaryModule.class),
        binder -> {
          binder.bind(RuntimeInfo.class).toInstance(new MockRuntimeInfo(numProcessors, directMemorySize, heapSize));
          JsonConfigProvider.bind(binder, "druid.processing", DruidProcessingConfig.class);
        }
    );
  }

  public static class MockRuntimeInfo extends RuntimeInfo
  {
    private final int availableProcessors;
    private final long maxHeapSize;
    private final long directSize;

    public MockRuntimeInfo(int availableProcessors, long directSize, long maxHeapSize)
    {
      this.availableProcessors = availableProcessors;
      this.directSize = directSize;
      this.maxHeapSize = maxHeapSize;
    }

    @Override
    public int getAvailableProcessors()
    {
      return availableProcessors;
    }

    @Override
    public long getTotalHeapSizeBytes()
    {
      return maxHeapSize;
    }

    @Override
    public long getMaxHeapSizeBytes()
    {
      return maxHeapSize;
    }

    @Override
    public long getDirectMemorySizeBytes()
    {
      return directSize;
    }
  }
}
