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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.projections.ProjectionMetadata;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Consolidated metadata for a V10 segment format container file. This is built by {@link SegmentFileBuilderV10}, and
 * is used by {@link SegmentFileMapperV10} to do its magic to turn these file bytes into segments.
 * <p>
 * V10 file format:
 * | version (byte) | meta compression (byte) | meta length (int) | meta json | container 0 | ... | container n |
 * <p>
 * where this class is the 'meta json' in ^ which describes the positions and possibly how to read the contents of the
 * containers which follow and any files contained within them.
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
  private final List<SegmentFileContainerMetadata> containers;
  private final Map<String, SegmentInternalFileMetadata> files;
  private final String interval;
  private final Map<String, ColumnDescriptor> columnDescriptors;
  private final Map<String, List<String>> columnFiles;
  private final List<ProjectionMetadata> projections;
  private final BitmapSerdeFactory bitmapEncoding;

  @JsonCreator
  public SegmentFileMetadata(
      @JsonProperty("containers") List<SegmentFileContainerMetadata> containers,
      @JsonProperty("files") Map<String, SegmentInternalFileMetadata> files,
      @JsonProperty("interval") @Nullable String interval,
      @JsonProperty("columnDescriptors") @Nullable Map<String, ColumnDescriptor> columnDescriptors,
      @JsonProperty("columnFiles") @Nullable Map<String, List<String>> columnFiles,
      @JsonProperty("projections") @Nullable List<ProjectionMetadata> projections,
      @JsonProperty("bitmapEncoding") @Nullable BitmapSerdeFactory bitmapEncoding
  )
  {
    this.containers = containers;
    this.files = internKeys(files);
    this.interval = interval;
    this.columnDescriptors = internKeys(columnDescriptors);
    this.columnFiles = internColumnFiles(columnFiles);
    this.projections = projections;
    this.bitmapEncoding = bitmapEncoding;

    // fail fast on inconsistent metadata: a columnFiles entry naming a file absent from the files map would
    // otherwise silently plan no download and only surface later as a null buffer during column deserialization
    if (this.columnFiles != null) {
      for (Map.Entry<String, List<String>> entry : this.columnFiles.entrySet()) {
        for (String file : entry.getValue()) {
          if (this.files == null || !this.files.containsKey(file)) {
            throw DruidException.defensive(
                "columnFiles for column[%s] references file[%s] which is not present in files",
                entry.getKey(),
                file
            );
          }
        }
      }
    }
  }

  @Nullable
  private static <V> Map<String, V> internKeys(@Nullable Map<String, V> map)
  {
    if (map == null) {
      return null;
    }
    final Map<String, V> interned = new HashMap<>();
    for (Map.Entry<String, V> entry : map.entrySet()) {
      interned.put(SmooshedFileMapper.STRING_INTERNER.intern(entry.getKey()), entry.getValue());
    }
    return interned;
  }

  @Nullable
  private static Map<String, List<String>> internColumnFiles(@Nullable Map<String, List<String>> map)
  {
    if (map == null) {
      return null;
    }
    final Map<String, List<String>> interned = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      final List<String> internedFiles = new ArrayList<>(entry.getValue().size());
      for (String file : entry.getValue()) {
        internedFiles.add(SmooshedFileMapper.STRING_INTERNER.intern(file));
      }
      interned.put(SmooshedFileMapper.STRING_INTERNER.intern(entry.getKey()), internedFiles);
    }
    return interned;
  }

  @JsonProperty
  public List<SegmentFileContainerMetadata> getContainers()
  {
    return containers;
  }

  @JsonProperty
  public Map<String, SegmentInternalFileMetadata> getFiles()
  {
    return files;
  }

  /**
   * The segment's declared interval (the bucket-aligned time range it covers), as supplied by the writer at build
   * time and serialized as an ISO-8601 interval string. May be wider than the actual data's time range, the start
   * typically reflects the schema's bucket minimum (e.g. start-of-day for a daily-granularity segment) and the end
   * is rounded up to the next query-granularity bucket boundary after the latest row. For exact, data-derived
   * bounds (e.g. for time-boundary queries) use {@link ProjectionMetadata#getMinTime} / {@link
   * ProjectionMetadata#getMaxTime} on the entries in {@link #projections}, which are populated by newer writers and
   * reflect the true per-projection min/max {@code __time} across all rows.
   */
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

  /**
   * Mapping of column name (same key space as {@link #getColumnDescriptors()}, prefixed by owning projection) to the
   * complete list of {@link #files} written for that column, in write order with the column's primary file first.
   * A column's list is self-contained: it names every internal file needed to deserialize the column via its
   * {@link ColumnDescriptor}, including sub-files whose names are otherwise only reconstructable from serde-specific
   * naming conventions (nested-format {@code .__field_N} and dictionary/index sub-files,
   * {@link org.apache.druid.segment.data.GenericIndexedWriter} multi-file splits, etc.).
   * <p>
   * Null for segments written before this field existed (readers must always tolerate its absence, falling back to
   * coarser on-demand loading, so this field stays optional forever); when present, every column in
   * {@link #getColumnDescriptors()} has a complete entry, since the writer attributes each column's files as it writes
   * them.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Map<String, List<String>> getColumnFiles()
  {
    return columnFiles;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<ProjectionMetadata> getProjections()
  {
    return projections;
  }
}
