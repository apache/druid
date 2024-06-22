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

package org.apache.druid.spectator.histogram;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;


/**
 * Aggregator to build Spectator style histograms.
 */
public class SpectatorHistogramAggregator implements Aggregator
{

  private final ColumnValueSelector selector;

  @GuardedBy("this")
  private final SpectatorHistogram counts;


  public SpectatorHistogramAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
    counts = new SpectatorHistogram();
  }

  @Override
  public void aggregate()
  {
    Object obj = selector.getObject();
    if (obj == null) {
      return;
    }
    if (obj instanceof SpectatorHistogram) {
      SpectatorHistogram other = (SpectatorHistogram) obj;
      synchronized (this) {
        counts.merge(other);
      }
    } else if (obj instanceof Number) {
      synchronized (this) {
        counts.insert((Number) obj);
      }
    } else {
      throw new IAE(
          "Expected a long or a SpectatorHistogramMap, but received [%s] of type [%s]",
          obj,
          obj.getClass()
      );
    }
  }

  @Nullable
  @Override
  public synchronized Object get()
  {
    return counts.isEmpty() ? null : counts;
  }

  @Override
  public synchronized float getFloat()
  {
    return counts.getSum();
  }

  @Override
  public synchronized long getLong()
  {
    return counts.getSum();
  }

  @Override
  public synchronized boolean isNull()
  {
    return counts.isEmpty();
  }

  @Override
  public synchronized void close()
  {

  }
}
