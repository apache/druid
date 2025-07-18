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

package org.apache.druid.sql.calcite.parser;

import org.apache.calcite.sql.SqlNode;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a parsed "main" (not SET) SQL statement, plus context derived from SET statements.
 */
public class StatementAndSetContext
{
  private final SqlNode mainStatement;
  private final Map<String, Object> setContext;

  public StatementAndSetContext(SqlNode mainStatement, Map<String, Object> setContext)
  {
    this.mainStatement = mainStatement;
    this.setContext = setContext;
  }

  public SqlNode getMainStatement()
  {
    return mainStatement;
  }

  public Map<String, Object> getSetContext()
  {
    return setContext;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StatementAndSetContext that = (StatementAndSetContext) o;
    return Objects.equals(mainStatement, that.mainStatement)
           && Objects.equals(setContext, that.setContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(mainStatement, setContext);
  }

  @Override
  public String toString()
  {
    return "StatementAndSetContext{" +
           "mainStatement=" + mainStatement +
           ", setContext=" + setContext +
           '}';
  }
}
