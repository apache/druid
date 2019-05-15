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
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.concurrent.GuardedBy;

/**
 * Aggregator that merges T-Digest based sketches generated from {@link TDigestBuildSketchAggregator}
 */
public class TDigestMergeSketchAggregator implements Aggregator
{
  private final ColumnValueSelector<MergingDigest> selector;

  @GuardedBy("this")
  private MergingDigest tdigestSketch;

  public TDigestMergeSketchAggregator(
      ColumnValueSelector<MergingDigest> selector,
      final Integer compression
  )
  {
    this.selector = selector;
    this.tdigestSketch = new MergingDigest(compression);
  }

  @Override
  public void aggregate()
  {
    final MergingDigest sketch = selector.getObject();
    if (sketch == null) {
      return;
    }
    synchronized (this) {
      this.tdigestSketch.add(sketch);
    }
  }

  @Override
  public synchronized Object get()
  {
    return tdigestSketch;
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

  @Override
  public synchronized void close()
  {
    tdigestSketch = null;
  }

}
