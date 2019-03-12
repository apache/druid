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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Aggregator to build tDigest sketches on numeric values.
 */
public class TDigestBuildSketchAggregator implements Aggregator
{

  private final ColumnValueSelector selector;

  // Default compression
  static final int DEFAULT_COMPRESSION = 50;

  @GuardedBy("this")
  private MergingDigest histogram;


  public TDigestBuildSketchAggregator(ColumnValueSelector selector, Integer compression)
  {
    this.selector = selector;
    if (compression != null) {
      this.histogram = new MergingDigest(compression);
    } else {
      this.histogram = new MergingDigest(DEFAULT_COMPRESSION);
    }
  }

  @Override
  public synchronized void aggregate()
  {
    if (selector.getObject() instanceof Number) {
      histogram.add(((Number) selector.getObject()).doubleValue());
    } else {
      throw new IAE("Expected a number, received "
                    + selector.getObject()
                    + " of type "
                    + selector.getObject());
    }
  }

  @Nullable
  @Override
  public synchronized Object get()
  {
    return histogram;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public synchronized void close()
  {
    histogram = null;
  }
}
