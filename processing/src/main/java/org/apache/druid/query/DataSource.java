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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.Set;

/**
 * Represents a source... of data... for a query. Analogous to the "FROM" clause in SQL.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TableDataSource.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TableDataSource.class, name = "table"),
    @JsonSubTypes.Type(value = QueryDataSource.class, name = "query"),
    @JsonSubTypes.Type(value = UnionDataSource.class, name = "union"),
    @JsonSubTypes.Type(value = JoinDataSource.class, name = "join"),
    @JsonSubTypes.Type(value = LookupDataSource.class, name = "lookup"),
    @JsonSubTypes.Type(value = InlineDataSource.class, name = "inline"),
    @JsonSubTypes.Type(value = GlobalTableDataSource.class, name = "globalTable")
})
public interface DataSource
{
  /**
   * Returns the names of all table datasources involved in this query. Does not include names for non-tables, like
   * lookups or inline datasources.
   */
  Set<String> getTableNames();

  /**
   * Returns datasources that this datasource depends on. Will be empty for leaf datasources like 'table'.
   */
  List<DataSource> getChildren();

  /**
   * Return a new DataSource, identical to this one, with different children. The number of children must be equal
   * to the number of children that this datasource already has.
   */
  DataSource withChildren(List<DataSource> children);

  /**
   * Returns true if queries on this dataSource are cacheable at both the result level and per-segment level.
   * Currently, dataSources that modify the behavior of per-segment processing are not cacheable (like 'join').
   * Nor are dataSources that do not actually reference segments (like 'inline'), since cache keys are always based
   * on segment identifiers.
   *
   * Note: Ideally, queries on 'join' datasources _would_ be cacheable, but we cannot currently do this due to lacking
   * the code necessary to compute cache keys properly.
   */
  boolean isCacheable();

  /**
   * Returns true if all servers have a full copy of this datasource. True for things like inline, lookup, etc, or
   * for queries of those.
   *
   * Currently this is coupled with joinability - if this returns true then the query engine expects there exists a
   * {@link org.apache.druid.segment.join.JoinableFactory} which might build a
   * {@link org.apache.druid.segment.join.Joinable} for this datasource directly. If a subquery 'inline' join is
   * required to join this datasource on the right hand side, then this value must be false for now.
   *
   * In the future, instead of directly using this method, the query planner and engine should consider
   * {@link org.apache.druid.segment.join.JoinableFactory#isDirectlyJoinable(DataSource)} when determining if the
   * right hand side is directly joinable, which would allow decoupling this property from joins.
   */
  boolean isGlobal();

  /**
   * Returns true if this datasource represents concrete data that can be scanned via a
   * {@link org.apache.druid.segment.Segment} adapter of some kind. True for e.g. 'table' but not for 'query' or 'join'.
   *
   * @see org.apache.druid.query.planning.DataSourceAnalysis#isConcreteBased() which uses this
   * @see org.apache.druid.query.planning.DataSourceAnalysis#isConcreteTableBased() which uses this
   */
  boolean isConcrete();
}
