/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;

public class TaskStorageConfig
{
  @JsonProperty
  @NotNull
  public Duration recentlyFinishedThreshold = new Period("PT24H").toStandardDuration();

  @JsonCreator
  public TaskStorageConfig(
      @JsonProperty("recentlyFinishedThreshold") Period period
  )
  {
    if(period != null) {
      this.recentlyFinishedThreshold = period.toStandardDuration();
    }
  }

  public Duration getRecentlyFinishedThreshold()
  {
    return recentlyFinishedThreshold;
  }
}
