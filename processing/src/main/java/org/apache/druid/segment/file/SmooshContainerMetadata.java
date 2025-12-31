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
 * Starting offset and size of a 'container' stored in a V10 segment file; think the V10 equivalent of V9's external
 * 'smoosh' files, e.g. 00000.smoosh.
 */
public class SmooshContainerMetadata
{
  private final long startOffset;
  private final long size;

  @JsonCreator
  public SmooshContainerMetadata(
      @JsonProperty("startOffset") long startOffset,
      @JsonProperty("size") long size
  )
  {
    this.startOffset = startOffset;
    this.size = size;
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
