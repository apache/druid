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
import org.apache.druid.query.MetricValueExtractor;

import java.util.Map;

/**
 */
public class DimensionAndMetricValueExtractor extends MetricValueExtractor
{
  private final Map<String, Object> value;

  @JsonCreator
  public DimensionAndMetricValueExtractor(Map<String, Object> value)
  {
    super(value);

    this.value = value;
  }

  public Object getDimensionValue(String dimension)
  {
    return value.get(dimension);
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

    DimensionAndMetricValueExtractor that = (DimensionAndMetricValueExtractor) o;

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

  @Override
  public String toString()
  {
    return "DimensionAndMetricValueExtractor{" +
           "value=" + value +
           '}';
  }
}
