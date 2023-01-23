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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A query that can compute window functions on top of a completely in-memory inline datasource or query results.
 * <p>
 * It relies on a set of Operators to work on the data that it is given.  As such, it doesn't actually encapsulate
 * any window-specific logic in-and-of-itself, but rather delegates everything to the operators.  This is because
 * this is also intended as the initial addition of more explicit Operators to the Druid code base.
 * <p>
 * The assumptions on the incoming data are defined by the operators.  At initial time of writing, there is a baked
 * in assumption that data has been sorted "correctly" before this runs.
 */
public class WindowOperatorQuery extends BaseQuery<RowsAndColumns>
{
  private final RowSignature rowSignature;
  private final List<OperatorFactory> operators;

  @JsonCreator
  public WindowOperatorQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("outputSignature") RowSignature rowSignature,
      @JsonProperty("operatorDefinition") List<OperatorFactory> operators
  )
  {
    super(dataSource, new LegacySegmentSpec(Intervals.ETERNITY), false, context);
    this.rowSignature = rowSignature;
    this.operators = operators;

    // At this point, we can also reach into a QueryDataSource and validate that the ordering expected by the
    // partitioning at least aligns with the ordering coming from the underlying query.  We unfortunately don't
    // have enough information to validate that the underlying ordering aligns with expectations for the actual
    // window operator queries, but maybe we could get that and validate it here too.
    if (!(dataSource instanceof QueryDataSource || dataSource instanceof InlineDataSource)) {
      throw new IAE("WindowOperatorQuery must run on top of a query or inline data source, got [%s]", dataSource);
    }
  }

  @JsonProperty("operatorDefinition")
  public List<OperatorFactory> getOperators()
  {
    return operators;
  }

  @JsonProperty("outputSignature")
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  @Nullable
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return Query.WINDOW_OPERATOR;
  }

  @Override
  public Query<RowsAndColumns> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new WindowOperatorQuery(
        getDataSource(),
        computeOverriddenContext(getContext(), contextOverride),
        rowSignature,
        operators
    );
  }

  @Override
  public Query<RowsAndColumns> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new UOE("Cannot override querySegmentSpec on window operator query. [%s]", spec);
  }

  @Override
  public Query<RowsAndColumns> withDataSource(DataSource dataSource)
  {
    return new WindowOperatorQuery(
        dataSource,
        getContext(),
        rowSignature,
        operators
    );
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
    if (!super.equals(o)) {
      return false;
    }
    WindowOperatorQuery that = (WindowOperatorQuery) o;
    return Objects.equals(rowSignature, that.rowSignature) && Objects.equals(
        operators,
        that.operators
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), rowSignature, operators);
  }

  @Override
  public String toString()
  {
    return "WindowOperatorQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", context=" + getContext() +
           ", rowSignature=" + rowSignature +
           ", operators=" + operators +
           '}';
  }
}
