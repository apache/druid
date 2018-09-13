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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamTaskReportData;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class TaskReportData extends SeekableStreamTaskReportData<Integer, Long>
{

  private final Map<Integer, Long> lag;

  public TaskReportData(
      String id,
      @Nullable Map<Integer, Long> startingOffsets,
      @Nullable Map<Integer, Long> currentOffsets,
      @Nullable DateTime startTime,
      Long remainingSeconds,
      TaskType type,
      @Nullable Map<Integer, Long> lag
  )
  {
    super(
        id,
        startingOffsets,
        currentOffsets,
        startTime,
        remainingSeconds,
        type
    );

    this.lag = lag;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<Integer, Long> getLag()
  {
    return lag;
  }

  @Override
  public String toString()
  {
    return "{" +
           "id='" + getId() + '\'' +
           (getStartingOffsets() != null ? ", startingOffsets=" + getStartingOffsets() : "") +
           (getCurrentOffsets() != null ? ", currentOffsets=" + getCurrentOffsets() : "") +
           ", startTime=" + getStartTime() +
           ", remainingSeconds=" + getRemainingSeconds() +
           (lag != null ? ", lag=" + lag : "") +
           '}';
  }
}
