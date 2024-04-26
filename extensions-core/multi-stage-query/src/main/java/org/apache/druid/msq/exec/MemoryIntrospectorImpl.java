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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;

import java.util.List;

/**
 * Production implementation of {@link MemoryIntrospector}.
 */
public class MemoryIntrospectorImpl implements MemoryIntrospector
{
  private static final Logger log = new Logger(MemoryIntrospectorImpl.class);

  private final LookupExtractorFactoryContainerProvider lookupProvider;
  private final long totalMemoryInJvm;
  private final int numQueriesInJvm;
  private final int numProcessorsInJvm;
  private final double usableMemoryFraction;

  /**
   * Create an introspector.
   *
   * @param lookupProvider       provider of lookups; we use this to subtract lookup size from total JVM memory when
   *                             computing usable memory
   * @param totalMemoryInJvm     maximum JVM heap memory
   * @param usableMemoryFraction fraction of JVM memory, after subtracting lookup overhead, that we consider usable
   *                             for multi-stage queries
   * @param numQueriesInJvm      maximum number of {@link Controller} or {@link Worker} that may run concurrently
   * @param numProcessorsInJvm   size of processing thread pool, typically {@link DruidProcessingConfig#getNumThreads()}
   */
  public MemoryIntrospectorImpl(
      final LookupExtractorFactoryContainerProvider lookupProvider,
      final long totalMemoryInJvm,
      final double usableMemoryFraction,
      final int numQueriesInJvm,
      final int numProcessorsInJvm
  )
  {
    this.lookupProvider = lookupProvider;
    this.totalMemoryInJvm = totalMemoryInJvm;
    this.numQueriesInJvm = numQueriesInJvm;
    this.numProcessorsInJvm = numProcessorsInJvm;
    this.usableMemoryFraction = usableMemoryFraction;
  }

  @Override
  public long totalMemoryInJvm()
  {
    return totalMemoryInJvm;
  }

  @Override
  public long usableMemoryInJvm()
  {
    final long totalMemory = totalMemoryInJvm();
    final long totalLookupFootprint = computeTotalLookupFootprint(true);
    return Math.max(
        0,
        (long) ((totalMemory - totalLookupFootprint) * usableMemoryFraction)
    );
  }

  @Override
  public long computeJvmMemoryRequiredForUsableMemory(long usableMemory)
  {
    final long totalLookupFootprint = computeTotalLookupFootprint(false);
    return (long) Math.ceil(usableMemory / usableMemoryFraction + totalLookupFootprint);
  }

  @Override
  public int numQueriesInJvm()
  {
    return numQueriesInJvm;
  }

  @Override
  public int numProcessorsInJvm()
  {
    return numProcessorsInJvm;
  }

  /**
   * Compute and return total estimated lookup footprint.
   *
   * Correctness of this approach depends on lookups being loaded *before* calling this method. Luckily, this is the
   * typical mode of operation, since by default druid.lookup.enableLookupSyncOnStartup = true.
   *
   * @param logFootprint whether footprint should be logged
   */
  private long computeTotalLookupFootprint(final boolean logFootprint)
  {
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

    if (logFootprint) {
      log.info("Lookup footprint: lookup count[%d], total bytes[%,d].", lookupNames.size(), lookupFootprint);
    }

    return lookupFootprint;
  }
}
