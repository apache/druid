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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * A deserialization aid used by {@link SegmentChangeRequestLoad}. The broker prunes the loadSpec from segments
 * for efficiency reasons, but the broker does need the loadSpec when it loads broadcast segments.
 *
 * This class always uses the non-pruning default {@link PruneSpecsHolder}.
 */
public class LoadableDataSegment extends DataSegment
{
  @JsonCreator
  public LoadableDataSegment(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      // use `Map` *NOT* `LoadSpec` because we want to do lazy materialization to prevent dependency pollution
      @JsonProperty("loadSpec") @Nullable Map<String, Object> loadSpec,
      @JsonProperty("dimensions")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> dimensions,
      @JsonProperty("metrics")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> metrics,
      @JsonProperty("shardSpec") @Nullable ShardSpec shardSpec,
      @JsonProperty("lastCompactionState") @Nullable CompactionState lastCompactionState,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size,
      @JacksonInject PruneSpecsHolder pruneSpecsHolder
  )
  {
    super(
        dataSource,
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        lastCompactionState,
        binaryVersion,
        size,
        PruneSpecsHolder.DEFAULT
    );

  }
}
