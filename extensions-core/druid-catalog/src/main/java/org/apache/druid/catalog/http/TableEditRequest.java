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
import org.apache.druid.data.input.InputSource;

import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = InputSource.TYPE_PROPERTY)
@JsonSubTypes(value = {
    @Type(name = "hideColumns", value = TableEditRequest.HideColumns.class),
    @Type(name = "unhideColumns", value = TableEditRequest.UnhideColumns.class),
    @Type(name = "dropColumns", value = TableEditRequest.DropColumns.class),
    @Type(name = "updateProperties", value = TableEditRequest.UpdateProperties.class),
    @Type(name = "updateColumns", value = TableEditRequest.UpdateColumns.class),
    @Type(name = "moveColumn", value = MoveColumn.class),
})
public class TableEditRequest
{
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
}
