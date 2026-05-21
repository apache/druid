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
 * Each container holds internal files belonging to exactly one named bundle, as declared at write time via
 * {@link SegmentFileBuilder#startFileBundle}. The {@link #bundle} field records that name so readers can attribute a
 * container to its bundle without parsing internal-file names. Containers written without an explicit
 * {@code startFileBundle} call are tagged with {@link SegmentFileBuilder#ROOT_BUNDLE_NAME}; that default value is
 * omitted from JSON output, so segments produced by writers pre-dating this field deserialize cleanly (missing
 * property normalizes to the default in the constructor).
 */
public class SegmentFileContainerMetadata
{
  private final long startOffset;
  private final long size;
  private final String bundle;

  @JsonCreator
  public SegmentFileContainerMetadata(
      @JsonProperty("startOffset") long startOffset,
      @JsonProperty("size") long size,
      @JsonProperty("bundle") @Nullable String bundle
  )
  {
    this.startOffset = startOffset;
    this.size = size;
    this.bundle = bundle == null ? SegmentFileBuilder.ROOT_BUNDLE_NAME : bundle;
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
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultBundleFilter.class)
  public String getBundle()
  {
    return bundle;
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
           && Objects.equals(bundle, that.bundle);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(startOffset, size, bundle);
  }

  @Override
  public String toString()
  {
    return "SegmentFileContainerMetadata{"
           + "startOffset=" + startOffset
           + ", size=" + size
           + ", bundle=" + bundle
           + '}';
  }

  /**
   * Jackson {@code valueFilter} that omits the {@code bundle} field from JSON when it carries the
   * {@link SegmentFileBuilder#ROOT_BUNDLE_NAME} default. Jackson invokes {@code equals(value)} against the filter
   * instance: returning {@code true} means "value equals default, omit it."
   */
  static final class DefaultBundleFilter
  {
    @Override
    public boolean equals(Object value)
    {
      return SegmentFileBuilder.ROOT_BUNDLE_NAME.equals(value);
    }

    @Override
    public int hashCode()
    {
      return 0;
    }
  }
}
