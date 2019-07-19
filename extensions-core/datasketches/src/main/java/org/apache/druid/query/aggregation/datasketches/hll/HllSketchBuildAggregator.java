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

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.List;

/**
 * This aggregator builds sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
public class HllSketchBuildAggregator implements Aggregator
{

  private final ColumnValueSelector<Object> selector;
  private HllSketch sketch;

  public HllSketchBuildAggregator(
      final ColumnValueSelector<Object> selector,
      final int lgK,
      final TgtHllType tgtHllType
  )
  {
    this.selector = selector;
    this.sketch = new HllSketch(lgK, tgtHllType);
  }

  /*
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public void aggregate()
  {
    final Object value = selector.getObject();
    if (value == null) {
      return;
    }
    synchronized (this) {
      updateSketch(sketch, value);
    }
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

  static void updateSketch(final HllSketch sketch, final Object value)
  {
    if (value instanceof Integer || value instanceof Long) {
      sketch.update(((Number) value).longValue());
    } else if (value instanceof Float || value instanceof Double) {
      sketch.update(((Number) value).doubleValue());
    } else if (value instanceof String) {
      sketch.update(((String) value).toCharArray());
    } else if (value instanceof List) {
      // noinspection unchecked
      List<String> list = (List<String>) value;
      for (String v : list) {
        sketch.update(v.toCharArray());
      }
    } else if (value instanceof char[]) {
      sketch.update((char[]) value);
    } else if (value instanceof byte[]) {
      sketch.update((byte[]) value);
    } else if (value instanceof int[]) {
      sketch.update((int[]) value);
    } else if (value instanceof long[]) {
      sketch.update((long[]) value);
    } else {
      throw new IAE("Unsupported type " + value.getClass());
    }
  }

}
