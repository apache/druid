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
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.projections.ProjectionMetadata;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Consolidated metadata for a V10 segment format container file. This is built by {@link SegmentFileBuilderV10}, and
 * is used by {@link SegmentFileMapperV10} to do its magic to turn these file bytes into segments.
 * <p>
 * V10 file format:
 * | version (byte) | meta compression (byte) | meta length (int) | meta json | chunk 0 | chunk 1 | ... | chunk n |
 * <p>
 * {@link #containers} and {@link #files} will always be present in a segment file, since a V10 segment file always
 * contains one or more internal files which are concatenated together, organized into containers (at least as long as
 * we use {@link java.nio.ByteBuffer}... we could drop {@link #containers} someday if we can shed our 2g limitation).
 * <p>
 * Optionally there will be addtional metadata about the schema and how to read these internal files if the file is a
 * load 'entry point' (e.g. {@link org.apache.druid.segment.IndexIO#V10_FILE_NAME}). The contents of the segment are
 * expressed as a list of {@link ProjectionMetadata} the first entry is always the base table projection (or
 * super-projection) of type {@link org.apache.druid.segment.projections.BaseTableProjectionSchema}. Additionally, a
 * mapping of column names to {@link ColumnDescriptor} is provided which defines how to deserialize the contents
 * of the {@link #files} into a {@link org.apache.druid.segment.column.ColumnHolder}. The column names here are prefixed
 * by the projection that they belong to, as the projections defined are used to provide the structure of how the
 * {@link #files} are organized during serialization to prevent naming conflicts between projections, or even between
 * operator defined contents such as column names and system defined data.
 */
public class SegmentFileMetadata
{
  private final List<SmooshContainerMetadata> containers;
  private final Map<String, SmooshFileMetadata> files;
  private final Map<String, ColumnDescriptor> columnDescriptors;
  private final String interval;
  private final BitmapSerdeFactory bitmapEncoding;
  private final List<ProjectionMetadata> projections;

  @JsonCreator
  public SegmentFileMetadata(
      @JsonProperty("containers") List<SmooshContainerMetadata> containers,
      @JsonProperty("files") Map<String, SmooshFileMetadata> files,
      @JsonProperty("interval") @Nullable String interval,
      @JsonProperty("bitmapEncoding") @Nullable BitmapSerdeFactory bitmapEncoding,
      @JsonProperty("columnDescriptors") @Nullable Map<String, ColumnDescriptor> columnDescriptors,
      @JsonProperty("projections") @Nullable List<ProjectionMetadata> projections
  )
  {
    this.containers = containers;
    this.files = files;
    this.columnDescriptors = columnDescriptors;
    this.interval = interval;
    this.bitmapEncoding = bitmapEncoding;
    this.projections = projections;
  }

  @JsonProperty
  public List<SmooshContainerMetadata> getContainers()
  {
    return containers;
  }

  @JsonProperty
  public Map<String, SmooshFileMetadata> getFiles()
  {
    return files;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getInterval()
  {
    return interval;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public BitmapSerdeFactory getBitmapEncoding()
  {
    return bitmapEncoding;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, ColumnDescriptor> getColumnDescriptors()
  {
    return columnDescriptors;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<ProjectionMetadata> getProjections()
  {
    return projections;
  }
}
