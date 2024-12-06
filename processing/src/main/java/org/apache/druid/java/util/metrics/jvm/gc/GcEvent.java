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

package org.apache.druid.java.util.metrics.jvm.gc;

import org.apache.druid.java.util.common.Pair;

import java.util.Optional;

public class GcEvent
{
  private static final String GC_YOUNG_GENERATION_NAME = "young";
  private static final String GC_OLD_GENERATION_NAME = "old";
  private static final String GC_ZGC_GENERATION_NAME = "zgc";
  private static final String GC_G1_CONCURRENT_GENERATION_NAME = "g1_concurrent";
  private static final String CMS_COLLECTOR_NAME = "cms";
  private static final String G1_COLLECTOR_NAME = "g1";
  private static final String PARALLEL_COLLECTOR_NAME = "parallel";
  private static final String SERIAL_COLLECTOR_NAME = "serial";
  private static final String ZGC_COLLECTOR_NAME = "zgc";
  private static final String SHENANDOAN_COLLECTOR_NAME = "shenandoah";

  public final String druidGenerationName;
  public final String druidCollectorName;
  public final String jvmGcName;
  public final Optional<String> jvmGcCause;

  public GcEvent(final String jvmGcName)
  {
    this.jvmGcName = jvmGcName;
    this.jvmGcCause = Optional.empty();

    final Pair<String, String> druidNames = convertEventName(jvmGcName);
    this.druidGenerationName = druidNames.lhs;
    this.druidCollectorName = druidNames.rhs;
  }

  public GcEvent(final String jvmGcName, final String jvmGcCause)
  {
    this.jvmGcName = jvmGcName;
    this.jvmGcCause = Optional.of(jvmGcCause);

    final Pair<String, String> druidNames = convertEventName(jvmGcName);
    this.druidGenerationName = druidNames.lhs;
    this.druidCollectorName = druidNames.rhs;
  }

  private static Pair<String, String> convertEventName(final String jvmGcName)
  {
    switch (jvmGcName) {
      // CMS
      case "ParNew":
        return new Pair<>(GC_YOUNG_GENERATION_NAME, CMS_COLLECTOR_NAME);
      case "ConcurrentMarkSweep":
        return new Pair<>(GC_OLD_GENERATION_NAME, CMS_COLLECTOR_NAME);

      // G1
      case "G1 Young Generation":
        return new Pair<>(GC_YOUNG_GENERATION_NAME, G1_COLLECTOR_NAME);
      case "G1 Old Generation":
        return new Pair<>(GC_OLD_GENERATION_NAME, G1_COLLECTOR_NAME);
      case "G1 Concurrent GC":
        return new Pair<>(GC_G1_CONCURRENT_GENERATION_NAME, G1_COLLECTOR_NAME);

      // Parallel
      case "PS Scavenge":
        return new Pair<>(GC_YOUNG_GENERATION_NAME, PARALLEL_COLLECTOR_NAME);
      case "PS MarkSweep":
        return new Pair<>(GC_OLD_GENERATION_NAME, PARALLEL_COLLECTOR_NAME);

      // Serial
      case "Copy":
        return new Pair<>(GC_YOUNG_GENERATION_NAME, SERIAL_COLLECTOR_NAME);
      case "MarkSweepCompact":
        return new Pair<>(GC_OLD_GENERATION_NAME, SERIAL_COLLECTOR_NAME);

      // zgc
      case "ZGC":
        return new Pair<>(GC_ZGC_GENERATION_NAME, ZGC_COLLECTOR_NAME);

      // Shenandoah
      case "Shenandoah Cycles":
        return new Pair<>(GC_YOUNG_GENERATION_NAME, SHENANDOAN_COLLECTOR_NAME);
      case "Shenandoah Pauses":
        return new Pair<>(GC_OLD_GENERATION_NAME, SHENANDOAN_COLLECTOR_NAME);

      default:
        return new Pair<>(jvmGcName, jvmGcName);
    }
  }

  public boolean isConcurrent()
  {
    return (
        jvmGcName.endsWith(" Cycles") ||
        druidGenerationName.equals(GC_G1_CONCURRENT_GENERATION_NAME) ||
        (jvmGcCause.isPresent() && jvmGcCause.get().equals("No GC")));
  }
}
