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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;

/**
 * Engine for running SQL queries.
 */
public interface SqlEngine
{
  /**
   * Name of this engine. Appears in user-visible messages.
   */
  String name();

  /**
   * Whether a feature applies to this engine or not.
   */
  boolean featureAvailable(EngineFeature feature, PlannerContext plannerContext);

  /**
   * Validates a provided query context. Returns quietly if the context is OK; throws {@link ValidationException}
   * if the context has a problem.
   */
  void validateContext(Map<String, Object> queryContext);

  /**
   * SQL row type that would be emitted by the {@link QueryMaker} from {@link #buildQueryMakerForSelect}.
   * Called for SELECT. Not called for EXPLAIN, which is handled by the planner itself.
   *
   * @param typeFactory      type factory
   * @param validatedRowType row type from Calcite's validator
   */
  RelDataType resultTypeForSelect(RelDataTypeFactory typeFactory, RelDataType validatedRowType);

  /**
   * SQL row type that would be emitted by the {@link QueryMaker} from {@link #buildQueryMakerForInsert}.
   * Called for INSERT and REPLACE. Not called for EXPLAIN, which is handled by the planner itself.
   *
   * @param typeFactory      type factory
   * @param validatedRowType row type from Calcite's validator
   */
  RelDataType resultTypeForInsert(RelDataTypeFactory typeFactory, RelDataType validatedRowType);

  /**
   * Create a {@link QueryMaker} for a SELECT query.
   *
   * @param relRoot        planned and validated rel
   * @param plannerContext context for this query
   *
   * @return an executor for the provided query
   *
   * @throws ValidationException if this engine cannot build an executor for the provided query
   */
  @SuppressWarnings("RedundantThrows")
  QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext) throws ValidationException;

  /**
   * Create a {@link QueryMaker} for an INSERT ... SELECT query.
   *
   * @param destination      destination for the INSERT portion of the query
   * @param relRoot          planned and validated rel for the SELECT portion of the query
   * @param plannerContext   context for this query
   *
   * @return an executor for the provided query
   *
   * @throws ValidationException if this engine cannot build an executor for the provided query
   */
  @SuppressWarnings("RedundantThrows")
  QueryMaker buildQueryMakerForInsert(
      IngestDestination destination,
      RelRoot relRoot,
      PlannerContext plannerContext
  ) throws ValidationException;
}
