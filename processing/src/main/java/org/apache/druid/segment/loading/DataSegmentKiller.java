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

package org.apache.druid.segment.loading;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.List;

/**
 * DataSegmentKiller knows how to kill segments from the Druid system.
 * Since any implementation of DataSegmentKiller is initialized when an ingestion job starts
 * if a deep storage extension is loaded even when that deep storage is actually not used,
 * implementations should avoid initializing the deep storage client immediately
 * but defer it until the deep storage client is actually used.
 */
@ExtensionPoint
public interface DataSegmentKiller
{
  Logger log = new Logger(DataSegmentKiller.class);

  static String descriptorPath(String path)
  {
    int lastPathSeparatorIndex = path.lastIndexOf('/');
    if (lastPathSeparatorIndex == -1) {
      throw new IAE("Invalid path: [%s], should contain '/'", path);
    }
    return path.substring(0, lastPathSeparatorIndex) + "/descriptor.json";
  }

  /**
   * Removes segment files (index and metadata) from deep storage.
   * @param segment the segment to kill
   * @throws SegmentLoadingException if the segment could not be completely removed
   */
  void kill(DataSegment segment) throws SegmentLoadingException;

  /**
   * Kills a list of segments from deep storage. The default implementation calls kill on the segments in a loop.
   * Implementers of this interface can leverage batch / bulk deletes to be more efficient. It is preferable to attempt
   * to delete all segments even if there is an issue with deleting a single one. This is up to implementers to
   * implement as putting a try catch around the default kill via iteration can be problematic if the client of the deep
   * storage is unable to authenticate itself and segment loading exception doesn't encode enough information in it to \
   * understand why it failed.
   * <p>
   * If a segment or segments does not exist in deep storage, the method should not throw an exception.
   * <p>
   * This version of kill must **NOT** require additional permissions on the deep storage beyond what
   * {@link #kill(DataSegment)} requires.
   * @param segments The list of segments to kill.
   * @throws SegmentLoadingException If there is an exception during deletion such as a segment in the list could not be
   * completely removed.
   */
  default void kill(List<DataSegment> segments) throws SegmentLoadingException
  {
    for (DataSegment segment : segments) {
      kill(segment);
    }
  }

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
