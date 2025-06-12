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

import java.util.List;

/**
 * Result of killing data segments using {@link DataSegmentKiller}.
 */
public class SegmentKillResult
{
  private static final SegmentKillResult EMPTY_INSTANCE = new SegmentKillResult(List.of());

  public static SegmentKillResult empty()
  {
    return EMPTY_INSTANCE;
  }

  private final List<String> deletedPaths;

  public SegmentKillResult(
      List<String> deletedPaths
  )
  {
    this.deletedPaths = deletedPaths;
  }

  public List<String> getDeletedPaths()
  {
    return deletedPaths;
  }
}
