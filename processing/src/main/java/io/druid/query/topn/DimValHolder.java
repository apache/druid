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

import java.util.Map;

/**
 */
public class DimValHolder
{
  private final Object topNMetricVal;
  private final String dimName;
  private final Object dimValIndex;
  private final Map<String, Object> metricValues;

  public DimValHolder(
      Object topNMetricVal,
      String dimName,
      Object dimValIndex,
      Map<String, Object> metricValues
  )
  {
    this.topNMetricVal = topNMetricVal;
    this.dimName = dimName;
    this.dimValIndex = dimValIndex;
    this.metricValues = metricValues;
  }

  public Object getTopNMetricVal()
  {
    return topNMetricVal;
  }

  public String getDimName()
  {
    return dimName;
  }

  public Object getDimValIndex()
  {
    return dimValIndex;
  }

  public Map<String, Object> getMetricValues()
  {
    return metricValues;
  }

  public static class Builder
  {
    private Object topNMetricVal;
    private String dimName;
    private Object dimValIndex;
    private Map<String, Object> metricValues;

    public Builder()
    {
      topNMetricVal = null;
      dimName = null;
      dimValIndex = null;
      metricValues = null;
    }

    public Builder withTopNMetricVal(Object topNMetricVal)
    {
      this.topNMetricVal = topNMetricVal;
      return this;
    }

    public Builder withDimName(String dimName)
    {
      this.dimName = dimName;
      return this;
    }

    public Builder withDimValIndex(Object dimValIndex)
    {
      this.dimValIndex = dimValIndex;
      return this;
    }

    public Builder withMetricValues(Map<String, Object> metricValues)
    {
      this.metricValues = metricValues;
      return this;
    }

    public DimValHolder build()
    {
      return new DimValHolder(topNMetricVal, dimName, dimValIndex, metricValues);
    }
  }
}
