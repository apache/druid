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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;


/**
 * Aggregator to build T-Digest sketches on numeric values.
 * It generally makes sense to use this aggregator during the ingestion time.
 * <p>
 * One can use this aggregator to build these sketches during query time too, just
 * that it will be slower and more resource intensive.
 */
public class TDigestSketchAggregator implements Aggregator
{

  private final ColumnValueSelector selector;

  @GuardedBy("this")
  private MergingDigest histogram;


  public TDigestSketchAggregator(ColumnValueSelector selector, @Nullable Integer compression)
  {
    this.selector = selector;
    if (compression != null) {
      this.histogram = new MergingDigest(compression);
    } else {
      this.histogram = new MergingDigest(TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION);
    }
  }

  @Override
  public void aggregate()
  {
    if (selector.getObject() instanceof Number) {
      synchronized (this) {
        histogram.add(((Number) selector.getObject()).doubleValue());
      }
    } else if (selector.getObject() instanceof MergingDigest) {
      synchronized (this) {
        histogram.add((MergingDigest) selector.getObject());
      }
    } else {
      TDigestSketchUtils.throwExceptionForWrongType(selector);
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
    throw new UnsupportedOperationException("Casting to float type is not supported");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Casting to long type is not supported");
  }

  @Override
  public synchronized void close()
  {
    histogram = null;
  }
}
