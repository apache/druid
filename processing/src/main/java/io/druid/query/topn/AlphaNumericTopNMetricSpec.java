/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

import com.davekoelle.alphanum.AlphanumComparator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class AlphaNumericTopNMetricSpec extends LexicographicTopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final String previousStop;

  @JsonCreator
  public AlphaNumericTopNMetricSpec(
      @JsonProperty("previousStop") String previousStop
  )
  {
    super(previousStop);
    this.previousStop = (previousStop == null) ? "" : previousStop;
  }

  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    return new AlphanumComparator();
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
    return builder;
  }

  @Override
  public String toString()
  {
    return "AlphaNumericTopNMetricSpec{" +
           "previousStop='" + previousStop + '\'' +
           '}';
  }
}
