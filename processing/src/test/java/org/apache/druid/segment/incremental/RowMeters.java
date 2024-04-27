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

package org.apache.druid.segment.incremental;

/**
 * Utility class to build {@link RowIngestionMetersTotals}, used in tests.
 */
public class RowMeters
{
  private long processedBytes;
  private long processedWithError;
  private long unparseable;
  private long thrownAway;

  /**
   * Creates a new {@link RowMeters}, that can be used to build an instance of
   * {@link RowIngestionMetersTotals}.
   */
  public static RowMeters with()
  {
    return new RowMeters();
  }

  public RowMeters bytes(long processedBytes)
  {
    this.processedBytes = processedBytes;
    return this;
  }

  public RowMeters errors(long processedWithError)
  {
    this.processedWithError = processedWithError;
    return this;
  }

  public RowMeters unparseable(long unparseable)
  {
    this.unparseable = unparseable;
    return this;
  }

  public RowMeters thrownAway(long thrownAway)
  {
    this.thrownAway = thrownAway;
    return this;
  }

  public RowIngestionMetersTotals totalProcessed(long processed)
  {
    return new RowIngestionMetersTotals(processed, processedBytes, processedWithError, thrownAway, unparseable);
  }
}
