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
import org.apache.druid.segment.ReferenceCountedObject;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Represents something that can be the right-hand side of a join.
 *
 * This class's most important method is {@link #makeJoinMatcher}. Its main user is
 * {@link HashJoinEngine#makeJoinCursor}.
 */
public interface Joinable extends ReferenceCountedObject
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
   * Searches a column from this Joinable for a particular value, finds rows that match,
   * and returns values of a second column for those rows.
   *
   * @param searchColumnName Name of the search column. This is the column that is being used in the filter
   * @param searchColumnValue Target value of the search column. This is the value that is being filtered on.
   * @param retrievalColumnName The column to retrieve values from. This is the column that is being joined against.
   * @param maxCorrelationSetSize Maximum number of values to retrieve. If we detect that more values would be
   *                              returned than this limit, return an empty set.
   * @param allowNonKeyColumnSearch If true, allow searchs on non-key columns. If this is false,
   *                                a search on a non-key column should return an empty set.
   * @return The set of correlated column values. If we cannot determine correlated values, return absent.
   *
   * In case either the search or retrieval column names are not found, this will return absent.
   */
  Optional<Set<String>> getCorrelatedColumnValues(
      String searchColumnName,
      String searchColumnValue,
      String retrievalColumnName,
      long maxCorrelationSetSize,
      boolean allowNonKeyColumnSearch
  );
}
