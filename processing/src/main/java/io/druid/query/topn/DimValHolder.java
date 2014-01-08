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
    private String dirName;
    private Object dimValIndex;
    private Map<String, Object> metricValues;

    public Builder()
    {
      topNMetricVal = null;
      dirName = null;
      dimValIndex = null;
      metricValues = null;
    }

    public Builder withTopNMetricVal(Object topNMetricVal)
    {
      this.topNMetricVal = topNMetricVal;
      return this;
    }

    public Builder withDirName(String dirName)
    {
      this.dirName = dirName;
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
      return new DimValHolder(topNMetricVal, dirName, dimValIndex, metricValues);
    }
  }
}
