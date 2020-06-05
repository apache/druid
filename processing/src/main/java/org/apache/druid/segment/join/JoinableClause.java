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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.ReferenceCountedObject;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents everything about a join clause except for the left-hand datasource. In other words, if the full join
 * clause is "t1 JOIN t2 ON t1.x = t2.x" then this class represents "JOIN t2 ON x = t2.x" -- it does not include
 * references to the left-hand "t1".
 *
 * Created from {@link org.apache.druid.query.planning.PreJoinableClause} by {@link Joinables#createSegmentMapFn}.
 */
public class JoinableClause implements ReferenceCountedObject
{
  private final String prefix;
  private final Joinable joinable;
  private final JoinType joinType;
  private final JoinConditionAnalysis condition;

  public JoinableClause(String prefix, Joinable joinable, JoinType joinType, JoinConditionAnalysis condition)
  {
    this.prefix = Joinables.validatePrefix(prefix);
    this.joinable = Preconditions.checkNotNull(joinable, "joinable");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
    this.condition = Preconditions.checkNotNull(condition, "condition");
  }

  /**
   * The prefix to apply to all columns from the Joinable. The idea is that during a join, any columns that start with
   * this prefix should be retrieved from our Joinable's {@link JoinMatcher#getColumnSelectorFactory()}. Any other
   * columns should be returned from the left-hand side of the join.
   *
   * The prefix can be any string, as long as it is nonempty and not itself a prefix of the reserved column name
   * {@code __time}.
   *
   * @see #getAvailableColumnsPrefixed() the list of columns from our {@link Joinable} with prefixes attached
   * @see #unprefix a method for removing prefixes
   */
  public String getPrefix()
  {
    return prefix;
  }

  /**
   * The right-hand Joinable.
   */
  public Joinable getJoinable()
  {
    return joinable;
  }

  /**
   * The type of join: LEFT, RIGHT, INNER, or FULL.
   */
  public JoinType getJoinType()
  {
    return joinType;
  }

  /**
   * The join condition. When referring to right-hand columns, it should include the prefix.
   */
  public JoinConditionAnalysis getCondition()
  {
    return condition;
  }

  /**
   * Returns a list of columns from the underlying {@link Joinable#getAvailableColumns()} method, with our
   * prefix ({@link #getPrefix()}) prepended.
   */
  public List<String> getAvailableColumnsPrefixed()
  {
    return joinable.getAvailableColumns().stream().map(columnName -> prefix + columnName).collect(Collectors.toList());
  }

  /**
   * Returns whether "columnName" can be retrieved from the {@link Joinable} represented by this clause (i.e., whether
   * it starts with {@code prefix} and has at least one other character beyond that).
   */
  public boolean includesColumn(final String columnName)
  {
    return Joinables.isPrefixedBy(columnName, prefix);
  }

  /**
   * Removes our prefix from "columnName". Must only be called if {@link #includesColumn} would have returned true
   * on this column name.
   */
  public String unprefix(final String columnName)
  {
    if (includesColumn(columnName)) {
      return columnName.substring(prefix.length());
    } else {
      throw new IAE("Column[%s] does not start with prefix[%s]", columnName, prefix);
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
    JoinableClause that = (JoinableClause) o;
    return Objects.equals(prefix, that.prefix) &&
           Objects.equals(joinable, that.joinable) &&
           joinType == that.joinType &&
           Objects.equals(condition, that.condition);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(prefix, joinable, joinType, condition);
  }

  @Override
  public String toString()
  {
    return "JoinableClause{" +
           "prefix='" + prefix + '\'' +
           ", joinable=" + joinable +
           ", joinType=" + joinType +
           ", condition=" + condition +
           '}';
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    return joinable.acquireReferences();
  }
}
