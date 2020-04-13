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

package org.apache.druid.emitter.opentsdb;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Map;

public class OpentsdbEvent
{

  @JsonProperty
  private final String metric;

  // timestamp in seconds
  @JsonProperty
  private final long timestamp;

  @JsonProperty
  private final Object value;

  @JsonProperty
  private final Map<String, Object> tags;

  public OpentsdbEvent(
      @JsonProperty("metric") String metric,
      @JsonProperty("timestamp") Long timestamp,
      @JsonProperty("value") Object value,
      @JsonProperty("tags") Map<String, Object> tags
  )
  {
    this.metric = Preconditions.checkNotNull(metric, "metric can not be null.");
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp can not be null.");
    this.value = Preconditions.checkNotNull(value, "value can not be null.");
    this.tags = Preconditions.checkNotNull(tags, "tags can not be null.");
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

    OpentsdbEvent that = (OpentsdbEvent) o;

    if (!metric.equals(that.metric)) {
      return false;
    }
    if (timestamp != that.timestamp) {
      return false;
    }
    if (!value.equals(that.value)) {
      return false;
    }
    return tags.equals(that.tags);
  }

  @Override
  public int hashCode()
  {
    int result = metric.hashCode();
    result = 31 * result + (int) timestamp;
    result = 31 * result + value.hashCode();
    result = 31 * result + tags.hashCode();
    return result;
  }

  public String getMetric()
  {
    return metric;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public Object getValue()
  {
    return value;
  }

  public Map<String, Object> getTags()
  {
    return tags;
  }
}
