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
import org.apache.curator.shaded.com.google.common.base.Strings;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

/**
 * Definition of a column within an input source. Columns here describe
 * the "as created" form of the columns: what is actually in the input.
 * Column definitions are descriptive (of the data we already have), not
 * proscriptive (of the columns we'd like to have, since Druid does not
 * create input columns.)
 */
public class InputColumnDefn extends ColumnDefn
{
  @JsonCreator
  public InputColumnDefn(
      @JsonProperty("name") String name,
      @JsonProperty("sqlType") String sqlType)
  {
    super(name, sqlType);
  }

  @Override
  public void validate()
  {
    super.validate();
    if (Strings.isNullOrEmpty(name)) {
      throw new IAE("Columns names cannot be empty");
    }
    if (Strings.isNullOrEmpty(sqlType)) {
      throw new IAE("Columns type is required: " + name);
    }
    if (!VALID_SQL_TYPES.containsKey(StringUtils.toUpperCase(sqlType))) {
      throw new IAE("Not a supported SQL type: " + sqlType);
    }
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
    InputColumnDefn other = (InputColumnDefn) o;
    return Objects.equals(this.name, other.name)
        && Objects.equals(this.sqlType, other.sqlType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, sqlType);
  }
}
