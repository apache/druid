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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

/**
 * Description of a detail datasource column and a rollup
 * dimension column.
 */
public class DatasourceColumnDefn extends ColumnDefn
{
  private final String TIME_COLUMN = "__time";

  @JsonCreator
  public DatasourceColumnDefn(
      @JsonProperty("name") String name,
      @JsonProperty("sqlType") String sqlType
  )
  {
    super(name, sqlType);
  }

  public static Builder builder(String name)
  {
    return new Builder(name);
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

  public static class Builder
  {
    private final String name;
    private String sqlType;
    private String aggFn;

    public Builder(String name)
    {
      this.name = name;
    }

    public Builder sqlType(String type)
    {
      this.sqlType = type;
      return this;
    }

    public Builder measure(String aggFn)
    {
      this.aggFn = aggFn;
      return this;
    }

    public DatasourceColumnDefn build()
    {
      if (aggFn == null) {
        return new DatasourceColumnDefn(
            name,
            sqlType
            );
      } else {
        return new MeasureColumnDefn(
            name,
            sqlType,
            aggFn
            );
      }
    }
  }
}
