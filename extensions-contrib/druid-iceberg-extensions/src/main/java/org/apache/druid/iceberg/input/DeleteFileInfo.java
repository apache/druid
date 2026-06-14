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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Serializable metadata about an Iceberg delete file associated with a data file.
 * Carried inside {@link IcebergFileTaskInputSource} so that workers can apply
 * deletes without catalog access.
 */
public class DeleteFileInfo
{
  public enum ContentType
  {
    POSITION("position"),
    EQUALITY("equality");

    private final String value;

    ContentType(final String value)
    {
      this.value = value;
    }

    @JsonValue
    public String getValue()
    {
      return value;
    }
  }

  private final String path;
  private final ContentType contentType;
  private final List<Integer> equalityFieldIds;
  private final String format;

  @JsonCreator
  public DeleteFileInfo(
      @JsonProperty("path") final String path,
      @JsonProperty("contentType") final ContentType contentType,
      @JsonProperty("equalityFieldIds") final List<Integer> equalityFieldIds,
      @JsonProperty("format") final String format
  )
  {
    this.path = path;
    this.contentType = contentType;
    this.equalityFieldIds = equalityFieldIds != null ? equalityFieldIds : Collections.emptyList();
    this.format = format != null ? format : "PARQUET";
  }

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  @JsonProperty
  public ContentType getContentType()
  {
    return contentType;
  }

  @JsonProperty
  public List<Integer> getEqualityFieldIds()
  {
    return equalityFieldIds;
  }

  @JsonProperty
  public String getFormat()
  {
    return format;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DeleteFileInfo that = (DeleteFileInfo) o;
    return Objects.equals(path, that.path)
           && contentType == that.contentType
           && Objects.equals(equalityFieldIds, that.equalityFieldIds)
           && Objects.equals(format, that.format);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(path, contentType, equalityFieldIds, format);
  }

  @Override
  public String toString()
  {
    return "DeleteFileInfo{"
           + "path='" + path + '\''
           + ", contentType=" + contentType
           + ", equalityFieldIds=" + equalityFieldIds
           + ", format='" + format + '\''
           + '}';
  }
}
