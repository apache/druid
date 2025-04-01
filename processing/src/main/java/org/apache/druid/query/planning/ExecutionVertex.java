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

package org.apache.druid.query.planning;

import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.join.JoinPrefixUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the native engine's execution vertex.
 *
 *
 * Multiple queries might be executed in one stage: <br/>
 * The GroupBy query could be collapsed at exection time).
 *
 * Dag of datasources: <br/>
 * an execution may process an entire dag of datasource in some cases
 * (joindatasource) ; or collapse some into the execution (filter)
 */
public class ExecutionVertex
{
  protected final Query<?> topQuery;
  protected final DataSource baseDataSource;
  protected final QuerySegmentSpec querySegmentSpec;
  protected final List<String> joinPrefixes;
  protected boolean allRightsAreGlobal;

  private ExecutionVertex(ExecutionVertexExplorer explorer)
  {
    this.topQuery = explorer.topQuery;
    this.baseDataSource = explorer.baseDataSource;
    this.querySegmentSpec = explorer.querySegmentSpec;
    this.joinPrefixes = explorer.joinPrefixes;
    this.allRightsAreGlobal = explorer.allRightsAreGlobal;
  }

  public static ExecutionVertex of(Query<?> query)
  {
    ExecutionVertexExplorer explorer = new ExecutionVertexExplorer(query);
    return new ExecutionVertex(explorer);
  }

  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  public boolean isProcessable()
  {
    return getBaseDataSource().isProcessable() && allRightsAreGlobal;
  }

  public boolean isTableBased()
  {
    return baseDataSource instanceof TableDataSource
        || (baseDataSource instanceof UnionDataSource &&
            baseDataSource.getChildren()
                .stream()
                .allMatch(ds -> ds instanceof TableDataSource));
  }

  static class ExecutionVertexExplorer extends ExecutionVertexShuttle
  {
    boolean discoveringBase = true;
    DataSource baseDataSource;
    QuerySegmentSpec querySegmentSpec;
    Query<?> topQuery;
    List<String> joinPrefixes = new ArrayList<String>();
    boolean allRightsAreGlobal = true;

    public ExecutionVertexExplorer(Query<?> query)
    {
      topQuery = query;
      traverse(query);
    }

    @Override
    protected boolean mayTraverseQuery(Query<?> query)
    {
      return true;
    }

    @Override
    protected boolean mayTraverseDataSource(EVNode node)
    {
      if (parents.size() < 2) {
        return true;
      }
      if (node.index != null && node.index > 0) {
        return false;
      }
      if (node.dataSource instanceof QueryDataSource) {
        EVNode parentNode = parents.get(parents.size() - 2);
        if (parentNode.isQuery()) {
          return parentNode.getQuery().mayCollapseQueryDataSource();
        }
      }
      if (node.dataSource instanceof UnionDataSource) {
        return false;
      }
      return true;
    }

    @Override
    protected DataSource visit(DataSource dataSource, boolean leaf)
    {
      if (discoveringBase) {
        baseDataSource = dataSource;
        discoveringBase = false;
      }

      if (!leaf && dataSource instanceof JoinDataSource) {
        JoinDataSource joinDataSource = (JoinDataSource) dataSource;
        joinPrefixes.add(joinDataSource.getRightPrefix());
      }
      if (leaf && !isLeftLeaning()) {
        allRightsAreGlobal &= dataSource.isGlobal();
      }
      return dataSource;
    }

    private boolean isLeftLeaning()
    {
      for (EVNode evNode : parents) {
        if (evNode.index != null && evNode.index != 0) {
          return false;
        }
      }
      return true;
    }

    @Override
    protected Query<?> visitQuery(Query<?> query)
    {
      if (querySegmentSpec == null && isLeftLeaning()) {
        querySegmentSpec = getQuerySegmentSpec(query);
      }
      return query;
    }

    private QuerySegmentSpec getQuerySegmentSpec(Query<?> query)
    {
      if (query instanceof BaseQuery) {
        BaseQuery<?> baseQuery = (BaseQuery<?>) query;
        if (baseQuery.getQuerySegmentSpec() != null) {
          return baseQuery.getQuerySegmentSpec();
        }
      }
      return new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY);
    }
  }

  public static ExecutionVertex ofIllegal(DataSource dataSource)
  {
    ScanQuery query = Druids
        .newScanQueryBuilder()
        .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
        .dataSource(dataSource)
        .build();
    return ExecutionVertex.of(query);
  }

  /**
   * Unwraps the {@link #getBaseDataSource()} if its a {@link TableDataSource}.
   *
   * @throws An
   *           error of type {@link DruidException.Category#DEFENSIVE} if the
   *           {@link #getBaseDataSource()} is not a table.
   *
   *           note that this may not be true even
   *           {@link #isConcreteAndTableBased()} is true - in cases when the
   *           base datasource is a {@link UnionDataSource} of
   *           {@link TableDataSource}.
   */
  public final TableDataSource getBaseTableDataSource()
  {
    if (baseDataSource instanceof TableDataSource) {
      return (TableDataSource) baseDataSource;
    } else {
      throw DruidException.defensive("Base dataSource[%s] is not a table!", baseDataSource);
    }
  }

  /**
   * The applicable {@link QuerySegmentSpec} for this vertex.
   *
   * There might be more queries inside a single vertex; so the outer one is not
   * necessary correct.
   */
  public QuerySegmentSpec getEffectiveQuerySegmentSpec()
  {
    Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec is null!");
    return querySegmentSpec;
  }

  public boolean canRunQueryUsingClusterWalker()
  {
    return isProcessable() && isTableBased();
  }

  public boolean canRunQueryUsingLocalWalker()
  {
    return isProcessable() && !isTableBased();
  }

  public boolean isJoin()
  {
    return !joinPrefixes.isEmpty();

  }

  public boolean isBaseColumn(String columnName)
  {
    for (String prefix : joinPrefixes) {
      if (JoinPrefixUtils.isPrefixedBy(columnName, prefix)) {
        return false;
      }
    }
    return true;
  }

  public Query buildQueryWithBaseDataSource(DataSource newBaseDataSource)
  {
    return new ReplaceBaseDataSource(baseDataSource, newBaseDataSource).traverse(topQuery);
  }

  static class ReplaceBaseDataSource extends ExecutionVertexShuttle
  {
    private DataSource newBaseDataSource;
    private DataSource oldBaseDataSource;

    public ReplaceBaseDataSource(DataSource oldBaseDataSource, DataSource newBaseDataSource)
    {
      this.oldBaseDataSource = oldBaseDataSource;
      this.newBaseDataSource = newBaseDataSource;
    }

    @Override
    protected boolean mayTraverseQuery(Query<?> query)
    {
      return true;
    }

    @Override
    protected boolean mayTraverseDataSource(EVNode evNode)
    {
      return true;
    }

    @Override
    protected DataSource visit(DataSource dataSource, boolean leaf)
    {
      if (dataSource == oldBaseDataSource) {
        return newBaseDataSource;
      }
      return dataSource;
    }

    @Override
    protected Query<?> visitQuery(Query<?> query)
    {
      return query;
    }
  }

  @Override
  public boolean equals(Object obj)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode()
  {
    throw new UnsupportedOperationException();
  }
}
