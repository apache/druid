/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

/**
 */
public class TopNAlgorithmSelector
{
  private final int cardinality;
  private final int numBytesPerRecord;

  private volatile boolean hasDimExtractionFn;
  private volatile boolean aggregateAllMetrics;
  private volatile boolean aggregateTopNMetricFirst;

  public TopNAlgorithmSelector(int cardinality, int numBytesPerRecord)
  {
    this.cardinality = cardinality;
    this.numBytesPerRecord = numBytesPerRecord;
  }

  public void setHasDimExtractionFn(boolean hasDimExtractionFn)
  {
    this.hasDimExtractionFn = hasDimExtractionFn;
  }

  public void setAggregateAllMetrics(boolean aggregateAllMetrics)
  {
    this.aggregateAllMetrics = aggregateAllMetrics;
  }

  public void setAggregateTopNMetricFirst(boolean aggregateTopNMetricFirst)
  {
    // These are just heuristics based on an analysis of where an inflection point may lie to switch
    // between different algorithms
    if (cardinality > 400000 && numBytesPerRecord > 100) {
      this.aggregateTopNMetricFirst = aggregateTopNMetricFirst;
    }
  }

  public boolean isHasDimExtractionFn()
  {
    return hasDimExtractionFn;
  }

  public boolean isAggregateAllMetrics()
  {
    return aggregateAllMetrics;
  }

  public boolean isAggregateTopNMetricFirst()
  {
    return aggregateTopNMetricFirst;
  }
}
