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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Production implementation of {@link MemoryIntrospector}.
 */
public class MemoryIntrospectorImpl implements MemoryIntrospector
{
  private static final Logger log = new Logger(MemoryIntrospectorImpl.class);
  private static final long LOOKUP_FOOTPRINT_INIT = Long.MIN_VALUE;

  private final long totalMemoryInJvm;
  private final double usableMemoryFraction;
  private final int numTasksInJvm;
  private final int numProcessingThreads;

  /**
   * Lookup footprint per task, set the first time {@link #memoryPerTask()} is called.
   */
  private volatile long lookupFootprint = LOOKUP_FOOTPRINT_INIT;

  @Nullable
  private final LookupExtractorFactoryContainerProvider lookupProvider;

  /**
   * Create an introspector.
   *
   * @param totalMemoryInJvm     maximum JVM heap memory
   * @param usableMemoryFraction fraction of JVM memory, after subtracting lookup overhead, that we consider usable
   *                             for {@link Controller} or {@link Worker}
   * @param numTasksInJvm        maximum number of {@link Controller} or {@link Worker} that may run concurrently
   * @param numProcessingThreads size of processing thread pool, typically {@link DruidProcessingConfig#getNumThreads()}
   * @param lookupProvider       provider of lookups; we use this to subtract lookup size from total JVM memory when
   *                             computing usable memory. Ignored if null. This is used once the first time
   *                             {@link #memoryPerTask()} is called, then the footprint is cached. As such, it provides
   *                             a point-in-time view only.
   */
  public MemoryIntrospectorImpl(
      final long totalMemoryInJvm,
      final double usableMemoryFraction,
      final int numTasksInJvm,
      final int numProcessingThreads,
      @Nullable final LookupExtractorFactoryContainerProvider lookupProvider
  )
  {
    this.totalMemoryInJvm = totalMemoryInJvm;
    this.usableMemoryFraction = usableMemoryFraction;
    this.numTasksInJvm = numTasksInJvm;
    this.numProcessingThreads = numProcessingThreads;
    this.lookupProvider = lookupProvider;
  }

  @Override
  public long totalMemoryInJvm()
  {
    return totalMemoryInJvm;
  }

  @Override
  public long memoryPerTask()
  {
    return Math.max(
        0,
        (long) ((totalMemoryInJvm - getTotalLookupFootprint()) * usableMemoryFraction) / numTasksInJvm
    );
  }

  @Override
  public long computeJvmMemoryRequiredForTaskMemory(long memoryPerTask)
  {
    if (memoryPerTask <= 0) {
      throw new IAE("Invalid memoryPerTask[%d], expected a positive number", memoryPerTask);
    }

    return (long) Math.ceil(memoryPerTask * numTasksInJvm / usableMemoryFraction) + getTotalLookupFootprint();
  }

  @Override
  public int numTasksInJvm()
  {
    return numTasksInJvm;
  }

  @Override
  public int numProcessingThreads()
  {
    return numProcessingThreads;
  }

  /**
   * Get a possibly-cached value of {@link #computeTotalLookupFootprint()}. The underlying computation method is
   * called just once, meaning this is not a good way to track the size of lookups over time. This is done to keep
   * memory calculations as consistent as possible.
   */
  private long getTotalLookupFootprint()
  {
    if (lookupFootprint == LOOKUP_FOOTPRINT_INIT) {
      synchronized (this) {
        if (lookupFootprint == LOOKUP_FOOTPRINT_INIT) {
          lookupFootprint = computeTotalLookupFootprint();
        }
      }
    }

    return lookupFootprint;
  }

  /**
   * Compute and return total estimated lookup footprint.
   *
   * Correctness of this approach depends on lookups being loaded *before* calling this method. Luckily, this is the
   * typical mode of operation, since by default druid.lookup.enableLookupSyncOnStartup = true.
   */
  private long computeTotalLookupFootprint()
  {
    if (lookupProvider == null) {
      return 0;
    }

    final List<String> lookupNames = ImmutableList.copyOf(lookupProvider.getAllLookupNames());

    long lookupFootprint = 0;

    for (final String lookupName : lookupNames) {
      final LookupExtractorFactoryContainer container = lookupProvider.get(lookupName).orElse(null);

      if (container != null) {
        try {
          final LookupExtractor extractor = container.getLookupExtractorFactory().get();
          lookupFootprint += extractor.estimateHeapFootprint();
        }
        catch (Exception e) {
          log.noStackTrace().warn(e, "Failed to load lookup[%s] for size estimation. Skipping.", lookupName);
        }
      }
    }

    log.info("Lookup footprint: lookup count[%d], total bytes[%,d].", lookupNames.size(), lookupFootprint);
    return lookupFootprint;
  }
}
