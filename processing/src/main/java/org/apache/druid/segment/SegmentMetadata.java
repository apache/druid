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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Encapsulates segment level information like numRows, schema fingerprint.
 */
public class SegmentMetadata
{
  @Nullable
  private final Long numRows;
  @Nullable
  private final String schemaFingerprint;

  @JsonCreator
  public SegmentMetadata(
      @JsonProperty("numRows") @Nullable Long numRows,
      @JsonProperty("schemaFingerprint") @Nullable String schemaFingerprint
  )
  {
    this.numRows = numRows;
    this.schemaFingerprint = schemaFingerprint;
  }

  @JsonProperty
  @Nullable
  public Long getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  @Nullable
  public String getSchemaFingerprint()
  {
    return schemaFingerprint;
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
    SegmentMetadata that = (SegmentMetadata) o;
    return Objects.equals(numRows, that.numRows) && Objects.equals(schemaFingerprint, that.schemaFingerprint);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(numRows, schemaFingerprint);
  }

  @Override
  public String toString()
  {
    return "SegmentStats{" +
           "numRows=" + numRows +
           ", fingerprint='" + schemaFingerprint + '\'' +
           '}';
  }
}
