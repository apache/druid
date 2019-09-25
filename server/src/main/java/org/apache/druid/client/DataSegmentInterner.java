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

package org.apache.druid.client;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.druid.timeline.DataSegment;

/**
 * Interns the DataSegment object in order to share the reference for same DataSegment.
 * It uses two separate interners for realtime and historical segments to prevent
 * overwriting the size of a segment which was served by a historical and later served
 * by another realtime server, since realtime server always publishes with size 0.
 */
public class DataSegmentInterner
{
  private static final Interner<DataSegment> REALTIME_INTERNER = Interners.newWeakInterner();
  private static final Interner<DataSegment> HISTORICAL_INTERNER = Interners.newWeakInterner();

  private DataSegmentInterner()
  {
    //No instantiation
  }

  public static DataSegment intern(DataSegment segment)
  {
    // A segment learns it's size and dimensions when it moves from a relatime to historical server
    // for that reason, we are using it's size as the indicator to decide whether to use REALTIME or
    // HISTORICAL interner.
    return segment.getSize() > 0 ? HISTORICAL_INTERNER.intern(segment) : REALTIME_INTERNER.intern(segment);
  }
}
