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

package org.apache.druid.server.compaction;

/**
 * Represents the mode of compaction for segment intervals.
 * <p>
 * This enum defines different compaction modes that can be applied to segments.
 */
public enum CompactionMode
{
  /**
   * Indicates that a full compaction should be performed on the segments.
   * This mode allows creating compaction candidates that will undergo complete compaction.
   */
  FULL_COMPACTION;
}
