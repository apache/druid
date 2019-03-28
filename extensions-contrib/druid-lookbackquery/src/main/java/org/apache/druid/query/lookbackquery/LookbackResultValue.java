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

package org.apache.druid.query.lookbackquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.joda.time.Period;

import java.util.HashMap;
import java.util.Map;

/**
 * Data structure to represent the result of a Lookback query
 */
public class LookbackResultValue
{

  private final Map<String, Object> measurementValues;
  private final Map<Period, Map<String, Object>> lookbackValues;

  public LookbackResultValue(
      Map<String, Object> value,
      Map<Period, Map<String, Object>> lookbackValues
  )
  {
    this.measurementValues = value;
    if (lookbackValues != null) {
      this.lookbackValues = new HashMap<>(lookbackValues.size());
      this.lookbackValues.putAll(lookbackValues);
    } else {
      this.lookbackValues = new HashMap<>(2);
    }
  }

  @JsonCreator
  public LookbackResultValue(
      Map<String, Object> value
  )
  {
    this(value, null);
  }


  @JsonValue
  public Map<String, Object> getMeasurementValues()
  {
    return measurementValues;
  }

  public Float getFloatField(String name)
  {
    final Object retVal = measurementValues.get(name);
    return retVal == null ? null : ((Number) retVal).floatValue();
  }

  public Double getDoubleField(String name)
  {
    final Object retVal = measurementValues.get(name);
    return retVal == null ? null : ((Number) retVal).doubleValue();
  }

  public Long getLongField(String name)
  {
    final Object retVal = measurementValues.get(name);
    return retVal == null ? null : ((Number) retVal).longValue();
  }

  public Object getMetric(String name)
  {
    return measurementValues.get(name);
  }

  public Map<Period, Map<String, Object>> getLookbackValues()
  {
    return lookbackValues;
  }

  public Map<String, Object> getLookbackValuesForKey(Period key)
  {
    return lookbackValues.get(key);
  }

  public void addLookbackValuesForKey(Period key, Map<String, Object> values)
  {
    lookbackValues.put(key, values);
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

    LookbackResultValue that = (LookbackResultValue) o;

    if (measurementValues != null
        ? !measurementValues.equals(that.measurementValues)
        : that.measurementValues != null) {
      return false;
    } else if (!lookbackValues.equals(that.lookbackValues)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int hash = measurementValues != null ? measurementValues.hashCode() : 0;
    hash = 31 * hash + lookbackValues.hashCode();
    return hash;
  }

  @Override
  public String toString()
  {
    return "LookbackResultValue{" +
           "measurementValues=" + measurementValues +
           ",lookbackValues=" + lookbackValues +
           "}";
  }

}
