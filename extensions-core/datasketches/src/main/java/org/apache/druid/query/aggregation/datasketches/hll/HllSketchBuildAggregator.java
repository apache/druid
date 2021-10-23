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

package org.apache.druid.query.aggregation.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * This aggregator builds sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
public class HllSketchBuildAggregator implements Aggregator
{
  private final Consumer<Supplier<HllSketch>> processor;
  private HllSketch sketch;

  public HllSketchBuildAggregator(
      final Consumer<Supplier<HllSketch>> processor,
      final int lgK,
      final TgtHllType tgtHllType
  )
  {
    this.processor = processor;
    this.sketch = new HllSketch(lgK, tgtHllType);
  }

  /*
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public synchronized void aggregate()
  {
    processor.accept(() -> sketch);
  }

  /*
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public synchronized Object get()
  {
    return sketch.copy();
  }

  @Override
  public void close()
  {
    sketch = null;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }
}
