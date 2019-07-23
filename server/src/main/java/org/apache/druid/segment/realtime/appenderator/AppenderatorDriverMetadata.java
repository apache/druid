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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.realtime.appenderator.SegmentWithState.SegmentState;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AppenderatorDriverMetadata
{
  private final Map<String, List<SegmentWithState>> segments;
  private final Map<String, String> lastSegmentIds;
  private final Object callerMetadata;

  @JsonCreator
  public AppenderatorDriverMetadata(
      @JsonProperty("segments") Map<String, List<SegmentWithState>> segments,
      @JsonProperty("lastSegmentIds") Map<String, String> lastSegmentIds,
      @JsonProperty("callerMetadata") Object callerMetadata,
      // Next two properties are for backwards compatibility, should be removed on versions greater than 0.12.x
      @JsonProperty("activeSegments") Map<String, List<SegmentIdWithShardSpec>> activeSegments,
      @JsonProperty("publishPendingSegments") Map<String, List<SegmentIdWithShardSpec>> publishPendingSegments
  )
  {
    Preconditions.checkState(
        segments != null || (activeSegments != null && publishPendingSegments != null),
        "Metadata should either have segments with state information or both active segments and publish pending "
        + "segments information. segments [%s], activeSegments [%s], publishPendingSegments [%s]",
        segments,
        activeSegments,
        publishPendingSegments
    );
    if (segments == null) {
      // convert old metadata to new one
      final Map<String, List<SegmentWithState>> newMetadata = new HashMap<>();
      final Set<SegmentId> activeSegmentsAlreadySeen = new HashSet<>(); // temp data structure

      activeSegments.forEach((String sequence, List<SegmentIdWithShardSpec> sequenceSegments) -> newMetadata.put(
          sequence,
          sequenceSegments
              .stream()
              .map(segmentIdentifier -> {
                activeSegmentsAlreadySeen.add(segmentIdentifier.asSegmentId());
                return SegmentWithState.newSegment(segmentIdentifier);
              })
              .collect(Collectors.toList())
      ));
      // publishPendingSegments is a superset of activeSegments
      publishPendingSegments.forEach((sequence, sequenceSegments) -> {
        List<SegmentWithState> segmentWithStates = newMetadata.computeIfAbsent(sequence, seq -> new ArrayList<>());
        sequenceSegments
            .stream()
            .filter(segmentIdentifier -> !activeSegmentsAlreadySeen.contains(segmentIdentifier.asSegmentId()))
            .map(segmentIdentifier -> SegmentWithState.newSegment(segmentIdentifier, SegmentState.APPEND_FINISHED))
            .forEach(segmentWithStates::add);
      });
      this.segments = newMetadata;
    } else {
      this.segments = segments;
    }
    this.lastSegmentIds = lastSegmentIds;
    this.callerMetadata = callerMetadata;
  }

  public AppenderatorDriverMetadata(
      Map<String, List<SegmentWithState>> segments,
      Map<String, String> lastSegmentIds,
      Object callerMetadata
  )
  {
    this(segments, lastSegmentIds, callerMetadata, null, null);
  }

  @JsonProperty
  public Map<String, List<SegmentWithState>> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public Map<String, String> getLastSegmentIds()
  {
    return lastSegmentIds;
  }

  @JsonProperty
  public Object getCallerMetadata()
  {
    return callerMetadata;
  }

  @Override
  public String toString()
  {
    return "AppenderatorDriverMetadata{" +
           "segments=" + segments +
           ", lastSegmentIds=" + lastSegmentIds +
           ", callerMetadata=" + callerMetadata +
           '}';
  }
}
