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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Relative starting offset and size of an internal 'file' of a {@link SmooshContainerMetadata}
 */
public class SmooshFileMetadata
{
  private final int container;
  private final long startOffset;
  private final long size;

  @JsonCreator
  public SmooshFileMetadata(
      @JsonProperty("container") int container,
      @JsonProperty("startOffset") long startOffset,
      @JsonProperty("size") long size
  )
  {
    this.container = container;
    this.startOffset = startOffset;
    this.size = size;
  }

  @JsonProperty
  public int getContainer()
  {
    return container;
  }

  @JsonProperty
  public long getStartOffset()
  {
    return startOffset;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }
}
