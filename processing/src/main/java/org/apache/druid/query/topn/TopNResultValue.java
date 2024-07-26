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

package org.apache.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class TopNResultValue implements Iterable<DimensionAndMetricValueExtractor>
{
  private final List<DimensionAndMetricValueExtractor> valueList;

  @JsonCreator
  public static TopNResultValue create(List<?> value)
  {
    if (value == null) {
      return new TopNResultValue(new ArrayList<>());
    }

    return new TopNResultValue(Lists.transform(
        value,
        (Function<Object, DimensionAndMetricValueExtractor>) input -> {
          if (input instanceof Map) {
            return new DimensionAndMetricValueExtractor((Map) input);
          } else if (input instanceof DimensionAndMetricValueExtractor) {
            return (DimensionAndMetricValueExtractor) input;
          } else {
            throw new IAE("Unknown type for input[%s]", input.getClass());
          }
        }
    ));
  }

  public TopNResultValue(List<DimensionAndMetricValueExtractor> valueList)
  {
    this.valueList = valueList;
  }

  @JsonValue
  public List<DimensionAndMetricValueExtractor> getValue()
  {
    return valueList;
  }

  @Override
  public Iterator<DimensionAndMetricValueExtractor> iterator()
  {
    return valueList.iterator();
  }

  @Override
  public String toString()
  {
    return "TopNResultValue{" +
           "valueList=" + valueList +
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

    return Objects.equals(valueList, that.valueList);
  }

  @Override
  public int hashCode()
  {
    return valueList != null ? valueList.hashCode() : 0;
  }
}
