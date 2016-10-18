/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNResultValue implements Iterable<DimensionAndMetricValueExtractor>
{
  private final List<DimensionAndMetricValueExtractor> value;

  @JsonCreator
  public TopNResultValue(
      List<?> value
  )
  {
    this.value = (value == null) ? Lists.<DimensionAndMetricValueExtractor>newArrayList() : Lists.transform(
        value,
        new Function<Object, DimensionAndMetricValueExtractor>()
        {
          @Override
          public DimensionAndMetricValueExtractor apply(@Nullable Object input)
          {
            if (input instanceof Map) {
              return new DimensionAndMetricValueExtractor((Map) input);
            } else if (input instanceof DimensionAndMetricValueExtractor) {
              return (DimensionAndMetricValueExtractor) input;
            } else {
              throw new IAE("Unknown type for input[%s]", input.getClass());
            }
          }
        }
    );
  }

  @JsonValue
  public List<DimensionAndMetricValueExtractor> getValue()
  {
    return value;
  }

  @Override
  public Iterator<DimensionAndMetricValueExtractor> iterator()
  {
    return value.iterator();
  }

  @Override
  public String toString()
  {
    return "TopNResultValue{" +
           "value=" + value +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TopNResultValue that = (TopNResultValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }
}
