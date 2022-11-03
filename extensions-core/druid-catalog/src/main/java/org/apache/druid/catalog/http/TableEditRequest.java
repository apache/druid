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

package org.apache.druid.catalog.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.catalog.model.ColumnSpec;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Set of "edit" operations that can be applied to a table via the REST API
 * "edit" message. Allows the user to apply selected changes without the
 * need to download the entire spec and without the need for optimistic
 * locking. See the subclasses for the specific set of supported operations.
 * <p>
 * These operations are used to deserialize the operation. The Jackson-provided
 * type property identifies the kind of operation. Validation of each request
 * is done in {@link TableEditor} to allow control over any errors returned to
 * the REST caller. (If the caller provide an invalid type, or a badly-formed
 * JSON object, then the error message will, unfortunately, be generic.)
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = TableEditRequest.TYPE_PROPERTY)
@JsonSubTypes(value = {
    @Type(name = "hideColumns", value = TableEditRequest.HideColumns.class),
    @Type(name = "unhideColumns", value = TableEditRequest.UnhideColumns.class),
    @Type(name = "dropColumns", value = TableEditRequest.DropColumns.class),
    @Type(name = "updateProperties", value = TableEditRequest.UpdateProperties.class),
    @Type(name = "updateColumns", value = TableEditRequest.UpdateColumns.class),
    @Type(name = "moveColumn", value = TableEditRequest.MoveColumn.class),
})
public class TableEditRequest
{
  public static final String TYPE_PROPERTY = "type";

  /**
   * Add the given column names to the property that lists the hidden columns.
   */
  public static class HideColumns extends TableEditRequest
  {
    @JsonProperty("columns")
    public final List<String> columns;

    @JsonCreator
    public HideColumns(@JsonProperty("columns") List<String> columns)
    {
      this.columns = columns;
    }
  }

  /**
   * Remove the given column names from the property that lists the hidden columns.
   */
  public static class UnhideColumns extends TableEditRequest
  {
    @JsonProperty("columns")
    public final List<String> columns;

    @JsonCreator
    public UnhideColumns(@JsonProperty("columns") List<String> columns)
    {
      this.columns = columns;
    }
  }

  /**
   * Remove one or more columns from the list of columns from this table.
   */
  public static class DropColumns extends TableEditRequest
  {
    @JsonProperty("columns")
    public final List<String> columns;

    @JsonCreator
    public DropColumns(@JsonProperty("columns") List<String> columns)
    {
      this.columns = columns;
    }
  }

  /**
   * Update the set of properties with the given changes. Properties with a null
   * value are removed from the table spec. Those with non-null values are updated.
   */
  public static class UpdateProperties extends TableEditRequest
  {
    @JsonProperty("properties")
    public final Map<String, Object> properties;

    @JsonCreator
    public UpdateProperties(@JsonProperty("properties") Map<String, Object> properties)
    {
      this.properties = properties;
    }
  }

  /**
   * Update the list of columns. Columns that match the name of existing columns
   * are used to update the column. Values provided here replace those in the table
   * spec in the DB. Properties are merged as in {@link UpdateProperties}. If the
   * column given here does not yet exist in the table spec, then it is added at the
   * end of the existing columns list.
   */
  public static class UpdateColumns extends TableEditRequest
  {
    @JsonProperty("columns")
    public final List<ColumnSpec> columns;

    @JsonCreator
    public UpdateColumns(@JsonProperty("columns") List<ColumnSpec> columns)
    {
      this.columns = columns;
    }
  }

  /**
   * Move a column within the list of columns for a TableSpec.
   */
  public static class MoveColumn extends TableEditRequest
  {
    public enum Position
    {
      FIRST,
      LAST,
      BEFORE,
      AFTER
    }

    @JsonProperty
    public final String column;
    @JsonProperty
    public final Position where;
    @Nullable
    @JsonProperty
    public final String anchor;

    @JsonCreator
    public MoveColumn(
        @JsonProperty("column") final String column,
        @JsonProperty("where") final Position where,
        @JsonProperty("anchor") @Nullable final String anchor
    )
    {
      this.column = column;
      this.where = where;
      this.anchor = anchor;
    }
  }
}
