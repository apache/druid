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

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.sql.calcite.planner.PlannerContext;

/**
 * Interface for creating {@link QueryMaker}, which in turn are used to execute Druid queries.
 */
public interface QueryMakerFactory
{
  /**
   * Create a {@link QueryMaker} for a SELECT query.
   *
   * @param relRoot        planned and validated rel
   * @param plannerContext context for this query
   *
   * @return an executor for the provided query
   *
   * @throws ValidationException if this factory cannot build an executor for the provided query
   */
  @SuppressWarnings("RedundantThrows")
  QueryMaker buildForSelect(RelRoot relRoot, PlannerContext plannerContext) throws ValidationException;

  /**
   * Create a {@link QueryMaker} for an INSERT ... SELECT query.
   *
   * @param targetDataSource datasource for the INSERT portion of the query
   * @param relRoot          planned and validated rel for the SELECT portion of the query
   * @param plannerContext   context for this query
   *
   * @return an executor for the provided query
   *
   * @throws ValidationException if this factory cannot build an executor for the provided query
   */
  QueryMaker buildForInsert(
      String targetDataSource,
      RelRoot relRoot,
      PlannerContext plannerContext
  ) throws ValidationException;
}
