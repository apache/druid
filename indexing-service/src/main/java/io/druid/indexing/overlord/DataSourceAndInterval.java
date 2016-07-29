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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.query.ordering.StringComparators;
import org.joda.time.Interval;

import java.util.Comparator;

/**
 */
public class DataSourceAndInterval
{
  private String dataSource;
  private Interval interval;

  //comparator to sort DataSourceAndInterval only by dataSource.
  public static final Comparator<DataSourceAndInterval> DATA_SOURCE_COMPARATOR = new Comparator<DataSourceAndInterval>()
  {
    @Override
    public int compare(DataSourceAndInterval o1, DataSourceAndInterval o2)
    {
      return StringComparators.LEXICOGRAPHIC.compare(o1.getDataSource(), o2.getDataSource());
    }
  };

  @JsonCreator
  public DataSourceAndInterval(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
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

    DataSourceAndInterval that = (DataSourceAndInterval) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    return interval.equals(that.interval);

  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + interval.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "DataSourceAndInterval{" +
           "dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           '}';
  }
}
