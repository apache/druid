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

package org.apache.druid.sql.calcite.run;

import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.planner.QueryHandler;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;

/**
 * Interface for executing Druid queries. Each one is created by a {@link SqlEngine} and is tied to a
 * specific SQL query.
 */
public interface QueryMaker
{
  /**
   * Executes a given Druid query, which is expected to correspond to the SQL query that this QueryMaker was originally
   * created for. The returned arrays match the row type given by {@link SqlEngine#resultTypeForSelect} or
   * {@link SqlEngine#resultTypeForInsert}, depending on the nature of the statement.
   */
  QueryResponse<Object[]> runQuery(DruidQuery druidQuery);


  /**
   * Marks that the {@link QueryMaker} supports executing
   * {@link DruidLogicalNode}-s.
   *
   * In case {@link QueryMaker} implements this interface - this approach will
   * be preferred to execute the query in {@link QueryHandler}.
   */
  public interface FromDruidLogical
  {
    /**
     * Runs the query represented by {@link DruidLogicalNode}.
     */
    QueryResponse<Object[]> runQuery(DruidLogicalNode newRoot);
  }
}
