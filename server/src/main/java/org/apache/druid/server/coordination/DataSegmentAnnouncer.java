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

import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;

public interface DataSegmentAnnouncer
{
  void announceSegment(DataSegment segment) throws IOException;

  void unannounceSegment(DataSegment segment) throws IOException;

  void announceSegments(Iterable<DataSegment> segments) throws IOException;

  void unannounceSegments(Iterable<DataSegment> segments) throws IOException;

  /**
   * Announces schema associated with all segments for the specified realtime task.
   *
   * @param taskId taskId
   * @param segmentSchemas absolute schema for all sinks, in case the client requests full sync.
   * @param segmentSchemasChange schema change for all sinks
   */
  void announceSegmentSchemas(
      String taskId,
      SegmentSchemas segmentSchemas,
      @Nullable SegmentSchemas segmentSchemasChange
  );

  /**
   * Removes schema associated with all segments for the specified realtime task.
   * @param taskId taskId
   */
  void removeSegmentSchemasForTask(String taskId);
}
