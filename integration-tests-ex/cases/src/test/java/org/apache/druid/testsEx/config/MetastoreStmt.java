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

package org.apache.druid.testsEx.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.RegExUtils;

/**
 * Represents a statement (query) to send to the Druid metadata
 * storage database (metastore) before running tests. At present,
 * each query is only a SQL statement. The statements are represented
 * as objects to allow for other options (such as ignoring failures,
 * etc.)
 * <p>
 * Metastore queries often include a JSON payload. The metastore wants
 * to store the payload in compact form without spaces. However, such
 * JSON is hard for humans to understand. So, the configuration file
 * should format the SQL statement and JSON for readability. This class
 * will "compactify" the statement prior to execution.
 */
public class MetastoreStmt
{
  private final String sql;

  @JsonCreator
  public MetastoreStmt(
      @JsonProperty("sql") String sql
  )
  {
    this.sql = sql;
  }

  @JsonProperty("sql")
  public String sql()
  {
    return sql;
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }

  /**
   * Convert the human-readable form of the statement in YAML
   * into the compact JSON form preferred in the DB. Also
   * compacts the SQL, but that's OK.
   */
  public String toSQL()
  {
    String stmt = RegExUtils.replaceAll(sql, "\n", " ");
    stmt = RegExUtils.replaceAll(stmt, " +", " ");
    stmt = RegExUtils.replaceAll(stmt, ": ", ":");
    stmt = RegExUtils.replaceAll(stmt, ", ", ",");
    stmt = RegExUtils.replaceAll(stmt, " }", "}");
    stmt = RegExUtils.replaceAll(stmt, "\\{ ", "{");
    return stmt;
  }
}
