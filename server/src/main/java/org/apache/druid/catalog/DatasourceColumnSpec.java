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

package org.apache.druid.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

/**
 * Description of a detail datasource column and a rollup
 * dimension column.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "detail", value = DatasourceColumnSpec.DetailColumnSpec.class),
    @Type(name = "dimension", value = DatasourceColumnSpec.DimensionSpec.class),
    @Type(name = "measure", value = DatasourceColumnSpec.MeasureSpec.class),
})
public abstract class DatasourceColumnSpec extends ColumnSpec
{
  private static final String TIME_COLUMN = "__time";

  @JsonCreator
  public DatasourceColumnSpec(
      @JsonProperty("name") String name,
      @JsonProperty("sqlType") String sqlType
  )
  {
    super(name, sqlType);
  }

  @Override
  public void validate()
  {
    super.validate();
    if (sqlType == null) {
      return;
    }
    if (TIME_COLUMN.equals(name)) {
      if (!"TIMESTAMP".equalsIgnoreCase(sqlType)) {
        throw new IAE("__time column must have type TIMESTAMP");
      }
    } else if (!VALID_SQL_TYPES.containsKey(StringUtils.toUpperCase(sqlType))) {
      throw new IAE("Not a supported SQL type: " + sqlType);
    }
  }

  public static class DetailColumnSpec extends DatasourceColumnSpec
  {
    @JsonCreator
    public DetailColumnSpec(
        @JsonProperty("name") String name,
        @JsonProperty("sqlType") String sqlType
    )
    {
      super(name, sqlType);
    }

    @Override
    protected ColumnKind kind()
    {
      return ColumnKind.DETAIL;
    }
  }

  public static class DimensionSpec extends DatasourceColumnSpec
  {
    @JsonCreator
    public DimensionSpec(
        @JsonProperty("name") String name,
        @JsonProperty("sqlType") String sqlType
    )
    {
      super(name, sqlType);
    }

    @Override
    protected ColumnKind kind()
    {
      return ColumnKind.DIMENSION;
    }
  }

  /**
   * Catalog definition of a measure (metric) column.
   */
  public static class MeasureSpec extends DatasourceColumnSpec
  {
    private final String aggregateFn;

    @JsonCreator
    public MeasureSpec(
        @JsonProperty("name") String name,
        @JsonProperty("sqlType") String sqlType,
        @JsonProperty("aggregateFn") String aggregateFn
    )
    {
      super(name, sqlType);
      this.aggregateFn = aggregateFn;
    }

    @Override
    protected ColumnKind kind()
    {
      return ColumnKind.MEASURE;
    }

    @JsonProperty("aggregateFn")
    public String aggregateFn()
    {
      return aggregateFn;
    }
  }
}
