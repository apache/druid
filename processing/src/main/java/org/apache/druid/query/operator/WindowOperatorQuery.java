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
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

import java.util.ArrayList;
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
  private static DataSource validateMaybeRewriteDataSource(DataSource dataSource, boolean hasLeafs)
  {
    if (hasLeafs) {
      return dataSource;
    }

    // We can re-write scan-style sub queries into an operator instead of doing the actual Scan query.  So, we
    // check for that and, if we are going to do the rewrite, then we return the sub datasource such that the
    // parent constructor in BaseQuery stores the actual data source that we want to be distributed to.

    // At this point, we could also reach into a QueryDataSource and validate that the ordering expected by the
    // partitioning at least aligns with the ordering coming from the underlying query.  We unfortunately don't
    // have enough information to validate that the underlying ordering aligns with expectations for the actual
    // window operator queries, but maybe we could get that and validate it here too.
    if (dataSource instanceof QueryDataSource) {
      final Query<?> subQuery = ((QueryDataSource) dataSource).getQuery();
      if (subQuery instanceof ScanQuery) {
        return subQuery.getDataSource();
      }
      return dataSource;
    } else if (dataSource instanceof InlineDataSource) {
      return dataSource;
    } else {
      throw new IAE("WindowOperatorQuery must run on top of a query or inline data source, got [%s]", dataSource);
    }
  }

  private final RowSignature rowSignature;
  private final List<OperatorFactory> operators;
  private final List<OperatorFactory> leafOperators;

  @JsonCreator
  public WindowOperatorQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec intervals,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("outputSignature") RowSignature rowSignature,
      @JsonProperty("operatorDefinition") List<OperatorFactory> operators,
      @JsonProperty("leafOperators") List<OperatorFactory> leafOperators
  )
  {
    super(
        validateMaybeRewriteDataSource(dataSource, leafOperators != null),
        intervals,
        false,
        context
    );
    this.rowSignature = rowSignature;
    this.operators = operators;

    if (leafOperators == null) {
      this.leafOperators = new ArrayList<>();
      // We have to double check again because this was validated in a static context before passing to the `super()`
      // and we cannot save state from that...  Ah well.

      if (dataSource instanceof QueryDataSource) {
        final Query<?> subQuery = ((QueryDataSource) dataSource).getQuery();
        if (subQuery instanceof ScanQuery) {
          ScanQuery scan = (ScanQuery) subQuery;

          ArrayList<ColumnWithDirection> ordering = new ArrayList<>();
          for (ScanQuery.OrderBy orderBy : scan.getOrderBys()) {
            ordering.add(
                new ColumnWithDirection(
                    orderBy.getColumnName(),
                    ScanQuery.Order.DESCENDING == orderBy.getOrder()
                    ? ColumnWithDirection.Direction.DESC
                    : ColumnWithDirection.Direction.ASC
                )
            );
          }
          if (ordering.isEmpty()) {
            ordering = null;
          }

          this.leafOperators.add(
              new ScanOperatorFactory(
                  null,
                  scan.getFilter(),
                  scan.getOffsetLimit(),
                  scan.getColumns(),
                  scan.getVirtualColumns().isEmpty() ? null : scan.getVirtualColumns(),
                  ordering
              )
          );
        }
      }
    } else {
      this.leafOperators = leafOperators;
    }
  }

  @JsonProperty("operatorDefinition")
  public List<OperatorFactory> getOperators()
  {
    return operators;
  }

  @JsonProperty("leafOperators")
  public List<OperatorFactory> getLeafOperators()
  {
    return leafOperators;
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
        getQuerySegmentSpec(),
        computeOverriddenContext(getContext(), contextOverride),
        rowSignature,
        operators,
        leafOperators
    );
  }

  @Override
  public Query<RowsAndColumns> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new WindowOperatorQuery(
        getDataSource(),
        spec,
        getContext(),
        rowSignature,
        operators,
        leafOperators
    );
  }

  @Override
  public Query<RowsAndColumns> withDataSource(DataSource dataSource)
  {
    return new WindowOperatorQuery(
        dataSource,
        getQuerySegmentSpec(),
        getContext(),
        rowSignature,
        operators,
        leafOperators
    );
  }

  public Query<RowsAndColumns> withOperators(List<OperatorFactory> operators)
  {
    return new WindowOperatorQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getContext(),
        rowSignature,
        operators,
        leafOperators
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
    return Objects.equals(rowSignature, that.rowSignature)
        && Objects.equals(operators, that.operators)
        && Objects.equals(leafOperators, that.leafOperators);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), rowSignature, operators, leafOperators);
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
           ", leafOperators=" + leafOperators +
           '}';
  }
}
