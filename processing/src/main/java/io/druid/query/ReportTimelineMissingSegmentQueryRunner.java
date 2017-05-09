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

package io.druid.query;

import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ReportTimelineMissingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final Map<String, List<SegmentDescriptor>> segmentDescMap;

  public ReportTimelineMissingSegmentQueryRunner(Map<String, List<SegmentDescriptor>> segmentDescMap)
  {
    this.segmentDescMap = segmentDescMap;
  }

  @Override
  public Sequence<T> run(
      Query<T> query, Map<String, Object> responseContext
  )
  {
    Map<String, List<SegmentDescriptor>> missingSegments =
        (Map<String, List<SegmentDescriptor>>) responseContext.computeIfAbsent(
            Result.MISSING_SEGMENTS_KEY, k -> new HashMap<>()
        );
    missingSegments.putAll(segmentDescMap);

    return Sequences.empty();
  }
}
