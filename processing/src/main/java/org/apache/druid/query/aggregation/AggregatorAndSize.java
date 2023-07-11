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

package org.apache.druid.query.aggregation;

/**
 * Encapsulates an {@link Aggregator} and the initial size in bytes required by
 * the Aggregator.
 */
public class AggregatorAndSize
{
  private final Aggregator aggregator;
  private final long initialSizeBytes;

  /**
   * @param aggregator       Aggregator
   * @param initialSizeBytes Initial size in bytes (including JVM object overheads)
   *                         required by the aggregator.
   */
  public AggregatorAndSize(Aggregator aggregator, long initialSizeBytes)
  {
    this.aggregator = aggregator;
    this.initialSizeBytes = initialSizeBytes;
  }

  public Aggregator getAggregator()
  {
    return aggregator;
  }

  /**
   * Initial size of the aggregator in bytes including JVM object overheads.
   */
  public long getInitialSizeBytes()
  {
    return initialSizeBytes;
  }
}
