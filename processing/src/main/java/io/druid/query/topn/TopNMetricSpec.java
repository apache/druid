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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyTopNMetricSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "numeric", value = NumericTopNMetricSpec.class),
    @JsonSubTypes.Type(name = "lexicographic", value = LexicographicTopNMetricSpec.class),
    @JsonSubTypes.Type(name = "inverted", value = InvertedTopNMetricSpec.class)
})
public interface TopNMetricSpec
{
  public void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs);

  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs);

  public TopNResultBuilder getResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      int threshold,
      Comparator comparator,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  );

  public byte[] getCacheKey();

  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder);

  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector);

  public String getMetricName(DimensionSpec dimSpec);
}
