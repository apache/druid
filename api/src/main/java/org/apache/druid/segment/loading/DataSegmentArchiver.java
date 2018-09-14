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
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;

@ExtensionPoint
public interface DataSegmentArchiver
{
  /**
   * Perform an archive task on the segment and return the resulting segment or null if there was no action needed.
   *
   * @param segment The source segment
   *
   * @return The segment after archiving or `null` if there was no archiving performed.
   *
   * @throws SegmentLoadingException on error
   */
  @Nullable
  DataSegment archive(DataSegment segment) throws SegmentLoadingException;

  /**
   * Perform the restore from an archived segment and return the resulting segment or null if there was no action
   *
   * @param segment The source (archived) segment
   *
   * @return The segment after it has been unarchived
   *
   * @throws SegmentLoadingException on error
   */
  @Nullable
  DataSegment restore(DataSegment segment) throws SegmentLoadingException;
}
