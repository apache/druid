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

package org.apache.druid.server.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains information related to the capability of a server to load segments, for example the number of threads
 * available.
 */
public class SegmentLoadingCapabilities
{
  private final int numLoadingThreads;
  private final int numTurboLoadingThreads;
  private final boolean supportsPartialLoad;

  @JsonCreator
  public SegmentLoadingCapabilities(
      @JsonProperty("numLoadingThreads") int numLoadingThreads,
      @JsonProperty("numTurboLoadingThreads") int numTurboLoadingThreads,
      @JsonProperty("supportsPartialLoad") boolean supportsPartialLoad
  )
  {
    this.numLoadingThreads = numLoadingThreads;
    this.numTurboLoadingThreads = numTurboLoadingThreads;
    this.supportsPartialLoad = supportsPartialLoad;
  }

  /**
   * Convenience for callers that don't care about the partial-load capability (older code paths and tests).
   * Defaults the capability to {@code false}.
   */
  public SegmentLoadingCapabilities(int numLoadingThreads, int numTurboLoadingThreads)
  {
    this(numLoadingThreads, numTurboLoadingThreads, false);
  }

  @JsonProperty
  public int getNumLoadingThreads()
  {
    return numLoadingThreads;
  }

  @JsonProperty
  public int getNumTurboLoadingThreads()
  {
    return numTurboLoadingThreads;
  }

  /**
   * Whether the server understands the partial-load {@code LoadSpec} wrapper wire formats this Druid version ships
   * with (e.g., {@code PartialProjectionLoadSpec}). Older historicals deserialize an unknown wrapper type and fail;
   * the coordinator consults this flag and degrades partial-load requests to full-loads when it is {@code false}.
   * Treated as a single capability across all built-in partial-load schemes since they are all core-loaded.
   */
  @JsonProperty
  public boolean isSupportsPartialLoad()
  {
    return supportsPartialLoad;
  }

  @Override
  public String toString()
  {
    return "SegmentLoadingCapabilities{" +
           "numLoadingThreads=" + numLoadingThreads +
           ", numTurboLoadingThreads=" + numTurboLoadingThreads +
           ", supportsPartialLoad=" + supportsPartialLoad +
           '}';
  }
}
