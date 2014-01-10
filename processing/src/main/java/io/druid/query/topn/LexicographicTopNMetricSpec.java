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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.primitives.UnsignedBytes;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public class LexicographicTopNMetricSpec implements TopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private static Comparator<String> comparator = new Comparator<String>()
  {
    @Override
    public int compare(String s, String s2)
    {
      return UnsignedBytes.lexicographicalComparator().compare(s.getBytes(Charsets.UTF_8), s2.getBytes(Charsets.UTF_8));
    }
  };

  private final String previousStop;

  @JsonCreator
  public LexicographicTopNMetricSpec(
      @JsonProperty("previousStop") String previousStop
  )
  {
    this.previousStop = (previousStop == null) ? "" : previousStop;
  }

  @Override
  public void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
  }

  @JsonProperty
  public String getPreviousStop()
  {
    return previousStop;
  }


  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    return comparator;
  }

  @Override
  public TopNResultBuilder getResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      int threshold,
      Comparator comparator
  )
  {
    return new TopNLexicographicResultBuilder(timestamp, dimSpec, threshold, previousStop, comparator);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] previousStopBytes = previousStop.getBytes(Charsets.UTF_8);

    return ByteBuffer.allocate(1 + previousStopBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(previousStopBytes)
                     .array();
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    builder.skipTo(previousStop);
    builder.ignoreAfterThreshold();
    return builder;
  }

  @Override
  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    selector.setAggregateAllMetrics(true);
  }

  @Override
  public String toString()
  {
    return "LexicographicTopNMetricSpec{" +
           "previousStop='" + previousStop + '\'' +
           '}';
  }
}
