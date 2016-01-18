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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.IndexSpec;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

@Deprecated
public class ConvertSegmentBackwardsCompatibleTask extends ConvertSegmentTask
{
  @JsonCreator
  public ConvertSegmentBackwardsCompatibleTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("force") Boolean force,
      @JsonProperty("validate") Boolean validate
  )
  {
    super(
        id == null ? ConvertSegmentTask.makeId(dataSource, interval) : id,
        dataSource,
        interval,
        segment,
        indexSpec,
        force == null ? false : force,
        validate ==null ? false : validate,
        null
    );
  }

  @Deprecated
  public static class SubTask extends ConvertSegmentTask.SubTask
  {
    @JsonCreator
    public SubTask(
        @JsonProperty("groupId") String groupId,
        @JsonProperty("segment") DataSegment segment,
        @JsonProperty("indexSpec") IndexSpec indexSpec,
        @JsonProperty("force") Boolean force,
        @JsonProperty("validate") Boolean validate
    )
    {
      super(groupId, segment, indexSpec, force, validate, null);
    }
  }
}
