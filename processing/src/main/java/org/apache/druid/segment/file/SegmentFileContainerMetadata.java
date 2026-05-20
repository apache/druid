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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Starting offset and size of a 'container' stored in a V10 segment file; think the V10 equivalent of V9's external
 * 'smoosh' files, e.g. 00000.smoosh.
 * <p>
 * Each container holds internal files belonging to at most one named file group, as declared at write time via
 * {@link SegmentFileBuilder#startFileGroup}. The {@link #fileGroup} field records that name so readers can attribute
 * a container to its group without parsing internal-file names. The field is {@code null} for containers written
 * without a {@code startFileGroup} call (or with {@code startFileGroup(null)}), and for containers from segments
 * produced by writers that pre-date this field; null serializes as a Jackson-omitted property so old segments
 * round-trip unchanged.
 */
public class SegmentFileContainerMetadata
{
  private final long startOffset;
  private final long size;
  @Nullable
  private final String fileGroup;

  @JsonCreator
  public SegmentFileContainerMetadata(
      @JsonProperty("startOffset") long startOffset,
      @JsonProperty("size") long size,
      @JsonProperty("fileGroup") @Nullable String fileGroup
  )
  {
    this.startOffset = startOffset;
    this.size = size;
    this.fileGroup = fileGroup;
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getFileGroup()
  {
    return fileGroup;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentFileContainerMetadata that = (SegmentFileContainerMetadata) o;
    return startOffset == that.startOffset
           && size == that.size
           && Objects.equals(fileGroup, that.fileGroup);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(startOffset, size, fileGroup);
  }

  @Override
  public String toString()
  {
    return "SegmentFileContainerMetadata{"
           + "startOffset=" + startOffset
           + ", size=" + size
           + ", fileGroup=" + fileGroup
           + '}';
  }
}
