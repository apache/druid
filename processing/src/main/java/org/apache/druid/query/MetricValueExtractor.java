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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

/**
 */
public class MetricValueExtractor
{
  private final Map<String, Object> value;

  @JsonCreator
  public MetricValueExtractor(
      Map<String, Object> value
  )
  {
    this.value = value;
  }

  @JsonValue
  public Map<String, Object> getBaseObject()
  {
    return value;
  }

  public Double getDoubleMetric(String name)
  {
    final Object retVal = value.get(name);
    return retVal == null ? null : ((Number) retVal).doubleValue();
  }

  public Float getFloatMetric(String name)
  {
    final Object retVal = value.get(name);
    return retVal == null ? null : ((Number) retVal).floatValue();
  }

  public Long getLongMetric(String name)
  {
    final Object retVal = value.get(name);
    return retVal == null ? null : ((Number) retVal).longValue();
  }

  public Object getMetric(String name)
  {
    return value.get(name);
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

    MetricValueExtractor that = (MetricValueExtractor) o;

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
    return "MetricValueExtractor{" +
           "value=" + value +
           '}';
  }
}
