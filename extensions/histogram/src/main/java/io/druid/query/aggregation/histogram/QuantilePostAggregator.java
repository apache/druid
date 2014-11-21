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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("quantile")
public class QuantilePostAggregator extends ApproximateHistogramPostAggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Double.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
    }
  };

  private final float probability;
  private String fieldName;

  @JsonCreator
  public QuantilePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probability") float probability
  )
  {
    super(name, fieldName);
    this.probability = probability;
    this.fieldName = fieldName;

    if (probability < 0 | probability > 1) {
      throw new IAE("Illegal probability[%s], must be strictly between 0 and 1", probability);
    }
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    final ApproximateHistogram ah = (ApproximateHistogram) values.get(this.getFieldName());
    return ah.getQuantiles(new float[]{this.getProbability()})[0];
  }

  @JsonProperty
  public float getProbability()
  {
    return probability;
  }

  @Override
  public String toString()
  {
    return "QuantilePostAggregator{" +
           "probability=" + probability +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
