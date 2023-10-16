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
import com.google.common.base.Preconditions;
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
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
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
  private final RowSignature rowSignature;
  private final List<OperatorFactory> operators;
  private final List<OperatorFactory> leafOperators;

  public static WindowOperatorQuery build(
      DataSource dataSource,
      QuerySegmentSpec intervals,
      Map<String, Object> context,
      RowSignature rowSignature,
      List<OperatorFactory> operators,
      VirtualColumns virtualColumns
  )
  {
    List<OperatorFactory> leafOperators = new ArrayList<OperatorFactory>();

    if (dataSource instanceof QueryDataSource) {
      final Query<?> subQuery = ((QueryDataSource) dataSource).getQuery();
      if (subQuery instanceof ScanQuery) {
        // transform the scan query into a leaf operator
        ScanQuery scan = (ScanQuery) subQuery;
        dataSource = subQuery.getDataSource();

        ArrayList<ColumnWithDirection> ordering = new ArrayList<>();
        for (ScanQuery.OrderBy orderBy : scan.getOrderBys()) {
          ordering.add(
              new ColumnWithDirection(
                  orderBy.getColumnName(),
                  ScanQuery.Order.DESCENDING == orderBy.getOrder()
                      ? ColumnWithDirection.Direction.DESC
                      : ColumnWithDirection.Direction.ASC));
        }

        leafOperators.add(
            new ScanOperatorFactory(
                null,
                scan.getFilter(),
                (int) scan.getScanRowsLimit(),
                scan.getColumns(),
                scan.getVirtualColumns(),
//                ImmutableList.<String> builder()
//                .addAll(scan.getColumns())
//                .addAll(virtualColumns.getColumnNames())
//                .build(),
//                vc_union(scan.getVirtualColumns(), virtualColumns),
                ordering));
      }
    } else if (dataSource instanceof InlineDataSource) {
      // ok
    } else {
      throw new IAE("WindowOperatorQuery must run on top of a query or inline data source, got [%s]", dataSource);
    }
    if(!virtualColumns.isEmpty()) {
      leafOperators.add(new ScanOperatorFactory(
          null,
          null,
          null,
          null,
          virtualColumns,
          null));
    }

    return new WindowOperatorQuery(dataSource, intervals, context, rowSignature, operators, leafOperators);
  }

  private static VirtualColumns vc_union(VirtualColumns virtualColumns, VirtualColumns virtualColumns2)
  {
    if (virtualColumns2.isEmpty()) {
      return virtualColumns;
    }

    VirtualColumn[] aa = virtualColumns.getVirtualColumns();
    VirtualColumn[] aa2 = virtualColumns2.getVirtualColumns();
    List<VirtualColumn> vcs = new ArrayList<VirtualColumn>();
    for (VirtualColumn virtualColumn : aa) {
      vcs.add(virtualColumn);

    }
    for (VirtualColumn virtualColumn : aa2) {
      vcs.add(virtualColumn);

    }
    return VirtualColumns.create(vcs);
  }

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
        dataSource,
        intervals,
        false,
        context
    );
    this.rowSignature = rowSignature;
    this.operators = operators;
    this.leafOperators = Preconditions.checkNotNull(leafOperators, "leafOperators may not be null at this point!");
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
