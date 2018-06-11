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

package io.druid.segment.loading;

import io.druid.guice.annotations.ExtensionPoint;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;

import java.io.IOException;

@ExtensionPoint
public interface DataSegmentKiller
{
  Logger log = new Logger(DataSegmentKiller.class);

  /**
   * Removes segment files (index and metadata) from deep storage.
   * @param segment the segment to kill
   * @throws SegmentLoadingException if the segment could not be completely removed
   */
  void kill(DataSegment segment) throws SegmentLoadingException;

  /**
   * A more stoic killer who doesn't throw a tantrum if things get messy. Use when killing segments for best-effort
   * cleanup.
   * @param segment the segment to kill
   */
  default void killQuietly(DataSegment segment)
  {
    try {
      kill(segment);
    }
    catch (Exception e) {
      log.debug(e, "Failed to kill segment %s", segment);
    }
  }

  /**
   * Like a nuke. Use wisely. Used by the 'reset-cluster' command, and of the built-in deep storage implementations, it
   * is only implemented by local and HDFS.
   */
  void killAll() throws IOException;
}
