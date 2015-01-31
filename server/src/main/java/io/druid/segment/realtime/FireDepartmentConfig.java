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

package io.druid.segment.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Period;

/**
 */
public class FireDepartmentConfig
{
  private final int maxRowsInMemory;
  private final Period intermediatePersistPeriod;

  @JsonCreator
  public FireDepartmentConfig(
      @JsonProperty("maxRowsInMemory") int maxRowsInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod
  )
  {
    this.maxRowsInMemory = maxRowsInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod;

    Preconditions.checkArgument(maxRowsInMemory > 0, "maxRowsInMemory[%s] should be greater than 0", maxRowsInMemory);
    Preconditions.checkNotNull(intermediatePersistPeriod, "intermediatePersistPeriod");
  }

  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }
}
