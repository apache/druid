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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.Joinables;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a join of two datasources.
 *
 * Logically, this datasource contains the result of:
 *
 * (1) prefixing all right-side columns with "rightPrefix"
 * (2) then, joining the left and (prefixed) right sides using the provided type and condition
 *
 * Any columns from the left-hand side that start with "rightPrefix", and are at least one character longer than
 * the prefix, will be shadowed. It is up to the caller to ensure that no important columns are shadowed by the
 * chosen prefix.
 *
 * When analyzed by {@link org.apache.druid.query.planning.DataSourceAnalysis}, the right-hand side of this datasource
 * will become a {@link org.apache.druid.query.planning.PreJoinableClause} object.
 */
public class JoinDataSource implements DataSource
{
  private final DataSource left;
  private final DataSource right;
  private final String rightPrefix;
  private final JoinConditionAnalysis conditionAnalysis;
  private final JoinType joinType;

  private JoinDataSource(
      DataSource left,
      DataSource right,
      String rightPrefix,
      JoinConditionAnalysis conditionAnalysis,
      JoinType joinType
  )
  {
    this.left = Preconditions.checkNotNull(left, "left");
    this.right = Preconditions.checkNotNull(right, "right");
    this.rightPrefix = Joinables.validatePrefix(rightPrefix);
    this.conditionAnalysis = Preconditions.checkNotNull(conditionAnalysis, "conditionAnalysis");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
  }

  @JsonCreator
  public static JoinDataSource create(
      @JsonProperty("left") DataSource left,
      @JsonProperty("right") DataSource right,
      @JsonProperty("rightPrefix") String rightPrefix,
      @JsonProperty("condition") String condition,
      @JsonProperty("joinType") JoinType joinType,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    return new JoinDataSource(
        left,
        right,
        StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
        JoinConditionAnalysis.forExpression(
            Preconditions.checkNotNull(condition, "condition"),
            StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
            macroTable
        ),
        joinType
    );
  }

  @Override
  public Set<String> getTableNames()
  {
    final Set<String> names = new HashSet<>();
    names.addAll(left.getTableNames());
    names.addAll(right.getTableNames());
    return names;
  }

  @JsonProperty
  public DataSource getLeft()
  {
    return left;
  }

  @JsonProperty
  public DataSource getRight()
  {
    return right;
  }

  @JsonProperty
  public String getRightPrefix()
  {
    return rightPrefix;
  }

  @JsonProperty
  public String getCondition()
  {
    return conditionAnalysis.getOriginalExpression();
  }

  public JoinConditionAnalysis getConditionAnalysis()
  {
    return conditionAnalysis;
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 2) {
      throw new IAE("Expected [2] children, got [%d]", children.size());
    }

    return new JoinDataSource(children.get(0), children.get(1), rightPrefix, conditionAnalysis, joinType);
  }

  @Override
  public boolean isCacheable()
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return left.isGlobal() && right.isGlobal();
  }

  @Override
  public boolean isConcrete()
  {
    return false;
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
    JoinDataSource that = (JoinDataSource) o;
    return Objects.equals(left, that.left) &&
           Objects.equals(right, that.right) &&
           Objects.equals(rightPrefix, that.rightPrefix) &&
           Objects.equals(conditionAnalysis, that.conditionAnalysis) &&
           joinType == that.joinType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(left, right, rightPrefix, conditionAnalysis, joinType);
  }

  @Override
  public String toString()
  {
    return "JoinDataSource{" +
           "left=" + left +
           ", right=" + right +
           ", rightPrefix='" + rightPrefix + '\'' +
           ", condition=" + conditionAnalysis +
           ", joinType=" + joinType +
           '}';
  }
}
