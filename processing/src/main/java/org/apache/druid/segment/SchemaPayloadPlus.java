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

import java.util.Objects;

/**
 * Wrapper over {@link SchemaPayload} to include {@code numRows} information.
 */
public class SchemaPayloadPlus
{
  private final SchemaPayload schemaPayload;
  private final Long numRows;

  @JsonCreator
  public SchemaPayloadPlus(
      @JsonProperty("schemaPayload") SchemaPayload schemaPayload,
      @JsonProperty("numRows") Long numRows
  )
  {
    this.numRows = numRows;
    this.schemaPayload = schemaPayload;
  }

  @JsonProperty
  public SchemaPayload getSchemaPayload()
  {
    return schemaPayload;
  }

  @JsonProperty
  public Long getNumRows()
  {
    return numRows;
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
    SchemaPayloadPlus that = (SchemaPayloadPlus) o;
    return Objects.equals(schemaPayload, that.schemaPayload)
           && Objects.equals(numRows, that.numRows);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schemaPayload, numRows);
  }

  @Override
  public String toString()
  {
    return "SegmentSchemaMetadata{" +
           "schemaPayload=" + schemaPayload +
           ", numRows=" + numRows +
           '}';
  }
}
