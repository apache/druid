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

package io.druid.query.datasourcemetadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

/**
 */
public class DataSourceMetadataResultValue
{
  private final DateTime maxIngestedEventTime;

  @JsonCreator
  public DataSourceMetadataResultValue(
      @JsonProperty("maxIngestedEventTime") DateTime maxIngestedEventTime
  )
  {
    this.maxIngestedEventTime = maxIngestedEventTime;
  }

  @JsonProperty
  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime;
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

    DataSourceMetadataResultValue that = (DataSourceMetadataResultValue) o;

    if (maxIngestedEventTime != null
        ? !maxIngestedEventTime.equals(that.maxIngestedEventTime)
        : that.maxIngestedEventTime != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return maxIngestedEventTime != null ? maxIngestedEventTime.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "DataSourceMetadataResultValue{" +
           "maxIngestedEventTime=" + maxIngestedEventTime +
           '}';
  }
}
