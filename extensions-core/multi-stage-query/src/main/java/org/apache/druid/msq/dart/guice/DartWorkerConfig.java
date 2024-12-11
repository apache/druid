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

package org.apache.druid.msq.dart.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.msq.exec.MemoryIntrospector;

/**
 * Runtime configuration for workers (which run on Historicals).
 */
public class DartWorkerConfig
{
  /**
   * By default, allocate up to 35% of memory for the MSQ framework. This accounts for additional overhead due to
   * native queries, and lookups (which aren't accounted for by the Dart {@link MemoryIntrospector}).
   */
  private static final double DEFAULT_HEAP_FRACTION = 0.35;

  public static final int AUTO = -1;

  @JsonProperty("concurrentQueries")
  private int concurrentQueries = AUTO;

  @JsonProperty("heapFraction")
  private double heapFraction = DEFAULT_HEAP_FRACTION;

  public int getConcurrentQueries()
  {
    return concurrentQueries;
  }

  public double getHeapFraction()
  {
    return heapFraction;
  }
}
