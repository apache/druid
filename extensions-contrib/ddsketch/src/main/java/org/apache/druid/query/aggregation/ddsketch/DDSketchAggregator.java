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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;


/**
 * Aggregator to build DDsketches on numeric values.
 * It generally makes sense to use this aggregator during the ingestion time.
 * <p>
 * One can use this aggregator to build these sketches during query time too, just
 * that it will be slower and more resource intensive.
 */
public class DDSketchAggregator implements Aggregator
{

  private final ColumnValueSelector selector;

  @GuardedBy("this")
  private DDSketch histogram;

  public DDSketchAggregator(ColumnValueSelector selector, @Nullable Double relativeError, @Nullable Integer numBins)
  {
    int effectiveNumBins = numBins != null ? numBins : DDSketchAggregatorFactory.DEFAULT_NUM_BINS;
    double effectiveRelativeError = relativeError != null ? relativeError : DDSketchAggregatorFactory.DEFAULT_RELATIVE_ERROR;
    this.selector = selector;
    this.histogram = DDSketches.collapsingLowestDense(effectiveRelativeError, effectiveNumBins);
  }

  @Override
  public void aggregate()
  {
    Object obj = selector.getObject();
    if (obj == null) {
      return;
    }
    synchronized (this) {
      if (obj instanceof Number) {
        this.histogram.accept(((Number) obj).doubleValue());
      } else if (obj instanceof DDSketch) {
        this.histogram.mergeWith((DDSketch) obj);
      } else {
        throw new IAE(
            "Expected a number or an instance of DDSketch, but received [%s] of type [%s]",
            obj,
            obj.getClass()
        );
      }
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
    this.histogram = null;
  }
}
