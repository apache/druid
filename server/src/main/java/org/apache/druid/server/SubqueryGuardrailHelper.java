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

package org.apache.druid.server;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * Aids the {@link ClientQuerySegmentWalker} compute the available heap size per query for materializing the inline
 * results from the subqueries
 */
public class SubqueryGuardrailHelper
{
  private static final double SUBQUERY_MEMORY_BYTES_FRACTION = 0.5;
  private static final Logger log = new Logger(SubqueryGuardrailHelper.class);


  public static final String UNLIMITED_LIMIT_VALUE = "unlimited";
  public static final String AUTO_LIMIT_VALUE = "auto";

  public static final Long UNLIMITED_LIMIT_REPRESENTATION = -1L;

  private final long autoLimitBytes;

  public SubqueryGuardrailHelper(
      final LookupExtractorFactoryContainerProvider lookupManager,
      final long maxMemoryInJvm,
      final int brokerNumHttpConnections
  )
  {
    final DateTime start = DateTimes.nowUtc();
    autoLimitBytes = computeLimitBytesForAuto(lookupManager, maxMemoryInJvm, brokerNumHttpConnections);
    final long startupTimeMs = new Interval(start, DateTimes.nowUtc()).toDurationMillis();

    log.info("Took [%d] ms to initialize the SubqueryGuardrailHelper.", startupTimeMs);

    if (startupTimeMs >= 10_000) {
      log.warn("Took more than 10 seconds to initialize the SubqueryGuardrailHelper. "
               + "This happens when the lookup sizes are very large. "
               + "Consider lowering the size of the lookups to reduce the initialization time."
      );
    }

    log.info("Each query has a memory limit of [%d] bytes to materialize its subqueries' results if auto "
             + "limit is used", autoLimitBytes);
  }

  public long convertSubqueryLimitStringToLong(final String maxSubqueryLimit)
  {
    if (UNLIMITED_LIMIT_VALUE.equalsIgnoreCase(maxSubqueryLimit)) {
      return UNLIMITED_LIMIT_REPRESENTATION;
    }
    if (AUTO_LIMIT_VALUE.equalsIgnoreCase(maxSubqueryLimit)) {
      return autoLimitBytes;
    }

    long retVal;
    try {
      retVal = Long.parseLong(maxSubqueryLimit);
    }
    catch (NumberFormatException e) {
      throw InvalidInput.exception(
          e,
          "Unable to parse the provided maxSubqueryLimit [%s] to a valid number. Valid values for the "
          + "maxSubqueryLimits can be 'auto', 'unlimited' or a positive number representing bytes to reserve.",
          maxSubqueryLimit
      );
    }

    // This can happen if the provided number is greater than Longs.MAX_VALUE
    if (retVal < 0) {
      throw InvalidInput.exception("Limit too large");
    }

    return retVal;
  }

  /**
   * Computes the byte limit when 'auto' is passed as a parameter. This computes the total heap space available
   * for the subquery inlining by getting a fraction of the total heap space in JVM, removing the size of the lookups,
   * and dividing it by the maximum concurrent queries that can run. Maximum concurrent queries that Druid can
   * run is usually limited by its broker's http threadpool size
   *
   * Consider a JVM running locally with 4 GB heap size, 20 Broker threads and 100 MB space required by the lookups.
   * Each query under 'auto' would then get 97.5 MB to materialize the results. Considering the default of 100,000 rows
   * and each row consuming 300 - 700 bytes of heap space, the subqueries would approximately consume between 30 MB to
   * 70 MB of data, which looks approximately equivalent to what we reserved with auto. This would wildly vary as we have
   * larger rows, but that's where the "auto" factor of subquery bytes come into play, where we would estimate by size
   * the number of rows that can be materialized based on the memory they consume.
   */
  private static long computeLimitBytesForAuto(
      final LookupExtractorFactoryContainerProvider lookupManager,
      final long maxMemoryInJvm,
      final int brokerNumHttpConnections
  )
  {
    long memoryInJvmWithoutLookups = maxMemoryInJvm - computeLookupFootprint(lookupManager);
    long memoryInJvmForSubqueryResultsInlining = (long) (memoryInJvmWithoutLookups * SUBQUERY_MEMORY_BYTES_FRACTION);
    long memoryInJvmForSubqueryResultsInliningPerQuery = memoryInJvmForSubqueryResultsInlining
                                                         / brokerNumHttpConnections;
    return Math.max(memoryInJvmForSubqueryResultsInliningPerQuery, 1L);
  }

  /**
   * Computes the size occupied by the lookups. If the size of the lookup cannot be computed, it skips over the lookup
   */
  private static long computeLookupFootprint(final LookupExtractorFactoryContainerProvider lookupManager)
  {

    if (lookupManager == null || lookupManager.getAllLookupNames() == null) {
      log.warn("Failed to get the lookupManager for estimating lookup size. Skipping.");
      return 0;
    }

    int lookupCount = 0;
    long lookupFootprint = 0;

    for (final String lookupName : lookupManager.getAllLookupNames()) {
      final LookupExtractorFactoryContainer container = lookupManager.get(lookupName).orElse(null);

      if (container != null) {
        try {
          final LookupExtractor extractor = container.getLookupExtractorFactory().get();
          lookupFootprint += extractor.estimateHeapFootprint();
          lookupCount++;
        }
        catch (Exception e) {
          log.noStackTrace().warn(e, "Failed to load lookup [%s] for size estimation. Skipping.", lookupName);
        }
      }
    }

    log.debug("Lookup footprint: [%d] lookups with [%,d] total bytes.", lookupCount, lookupFootprint);

    return lookupFootprint;
  }
}
