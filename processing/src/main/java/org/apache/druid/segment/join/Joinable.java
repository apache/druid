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

package org.apache.druid.segment.join;

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Represents something that can be the right-hand side of a join.
 *
 * This class's most important method is {@link #makeJoinMatcher}. Its main user is
 * {@link HashJoinEngine#makeJoinCursor}.
 */
public interface Joinable
{
  int CARDINALITY_UNKNOWN = -1;

  /**
   * Returns the list of columns that this Joinable offers.
   */
  List<String> getAvailableColumns();

  /**
   * Returns the cardinality of "columnName", or {@link #CARDINALITY_UNKNOWN} if not known. May be used at query
   * time to trigger optimizations.
   *
   * If not {@link #CARDINALITY_UNKNOWN}, this must match the cardinality of selectors returned by the
   * {@link ColumnSelectorFactory#makeDimensionSelector} method of this joinable's
   * {@link JoinMatcher#getColumnSelectorFactory()} .
   */
  int getCardinality(String columnName);

  /**
   * Returns capabilities for one of this Joinable's columns.
   *
   * @param columnName column name
   *
   * @return capabilities, or null if the columnName is not one of this Joinable's columns
   */
  @Nullable
  ColumnCapabilities getColumnCapabilities(String columnName);

  /**
   * Creates a JoinMatcher that can be used to implement a join onto this Joinable.
   *
   * @param leftColumnSelectorFactory column selector factory that allows access to the left-hand side of the join
   * @param condition                 join condition for the matcher
   * @param remainderNeeded           whether or not {@link JoinMatcher#matchRemainder()} will ever be called on the
   *                                  matcher. If we know it will not, additional optimizations are often possible.
   *
   * @return the matcher
   */
  JoinMatcher makeJoinMatcher(
      ColumnSelectorFactory leftColumnSelectorFactory,
      JoinConditionAnalysis condition,
      boolean remainderNeeded
  );

  /**
   * Given a main column name and value, return all the values of the column denoted by correlationColumnName
   * that appear in rows where the main column has the provided main column value.
   *
   * This is used for rewriting filter clauses when pushing filters down to the base table during join query processing.
   *
   * @param mainColumnName Name of the main column
   * @param mainColumnValue Target value of the main column
   * @param correlationColumnName The column to get correlated values from
   * @return The set of correlated column values. If we cannot determine correlated values, return an empty set.
   */
  Set<String> getCorrelatedColumnValues(
      String mainColumnName,
      String mainColumnValue,
      String correlationColumnName
  );
}
