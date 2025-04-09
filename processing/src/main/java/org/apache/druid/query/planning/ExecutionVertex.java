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
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.join.JoinPrefixUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Represents the native engine's execution vertex - the execution unit it may execute in one execution cycle.
 *
 * An execution cycle is one call to either CachingClusteredClient or LocalQuerySegmentWalker.
 *
 * This minimal Vertex a single {@link Query} with an input {@link DataSource}.
 *
 * However there might be more complicated cases.
 *
 * Multiple queries could be executed in one stage: <br/>
 * the GroupBy query could be collapsed at execution time.
 *
 * For example: <br/>
 * SELECT COUNT(*) FROM (SELECT string_first_added FROM druid.wikipedia_first_last GROUP BY 1)
 *
 * Will have 2 groupby queries; but they will be executed together - as that's suppoerted with {@link QueryToolChest#canPerformSubquery(Query)}.
 *
 * There could be a DAG of datasources: <br/>
 * an execution may process a complex dag of datasources when
 * {@link JoinDataSource}-es are present.
 *
 * For example: <br/>
 * SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)
 * inserts an UnnestDataSource into the query plan to execute the unnest operation.
 *
 * In case of a set of join-s:
 * SELECT t1.dim1 FROM foo t1 JOIN foo t2 ON (t1.dim1=t2.dim2) JOIN foo t3 ON (t1.dim1=t3.dim2)
 * There will be 2 JoinDataSource objects.
 * <pre>
 * Query
 *   JoinDataSource       - foo t3
 *     JoinDataSource     - foo t2
 *       TableDataSource  - foo t1
 * <pre>
 * Which will be executed together - as the JoinDataSource-es are applied via segment mapping.
 *
 * Every vertex has a base datasource - which could benefit from the advanced filtering techniques the cursors / walkers may provide.
 */
public class ExecutionVertex
{
  /** The top level query this vertex is describing. */
  protected final Query<?> topQuery;
  /** The base datasource which will be read during the execution.  */
  protected final DataSource baseDataSource;
  /** The effective {@link QuerySegmentSpec} of this vertex - this might be more restrictive that what the {@link Query} has.*/
  protected final QuerySegmentSpec querySegmentSpec;
  /** Retained for compatibility with earlier implementation. See {@link #isBaseColumn(String)} */
  protected final List<String> joinPrefixes;
  /** Retained for compatibility with earlier implementation. */
  protected boolean allRightsAreGlobal;

  private ExecutionVertex(ExecutionVertexExplorer explorer)
  {
    this.topQuery = explorer.topQuery;
    this.baseDataSource = explorer.baseDataSource;
    this.querySegmentSpec = explorer.querySegmentSpec;
    this.joinPrefixes = explorer.joinPrefixes;
    this.allRightsAreGlobal = explorer.allRightsAreGlobal;
  }

  /**
   * Identifies the vertex for the given query.
   */
  public static ExecutionVertex of(Query<?> query)
  {
    ExecutionVertexExplorer explorer = new ExecutionVertexExplorer(query);
    return new ExecutionVertex(explorer);
  }

  /**
   * The base datasource input of this vertex.
   */
  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  /**
   * Decides if this vertex is directly executable.
   *
   * A vertex is directly executable if it can be executed without any further processing.
   * See also: {@link DataSource#isProcessable()}.
   */
  public boolean isProcessable()
  {
    return getBaseDataSource().isProcessable() && allRightsAreGlobal;
  }

  /**
   * The vertex directly reads real tables.
   */
  public boolean isTableBased()
  {
    return baseDataSource instanceof TableDataSource
        || (baseDataSource instanceof UnionDataSource &&
            baseDataSource.getChildren()
                .stream()
                .allMatch(ds -> ds instanceof TableDataSource));
  }

  /**
   * Explores the vertex and collects all details.
   */
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

  /**
   * Builds the {@link ExecutionVertex} around a {@link DataSource}.
   *
   * Kept for backward compatibility reasons - incorporating
   * {@link ExecutionVertex} into Filtration will make this obsolete.
   */
  public static ExecutionVertex ofDataSource(DataSource dataSource)
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

  /**
   * Decides if the query can be executed using the cluster walker.
   */
  public boolean canRunQueryUsingClusterWalker()
  {
    return isProcessable() && isTableBased();
  }

  /**
   * Decides if the query can be executed using the local walker.
   */
  public boolean canRunQueryUsingLocalWalker()
  {
    return isProcessable() && !isTableBased();
  }

  /**
   * Decides if the execution time segment mapping function will be expensive.
   */
  public boolean isSegmentMapFunctionExpensive()
  {
    boolean hasJoin = !joinPrefixes.isEmpty();
    return hasJoin;
  }

  /**
   * Answers if the given column is coming from the base datasource or not.
   *
   * Retained for backward compatibility for now. The approach taken here relies
   * on join prefixes - which might classify the output of a
   * {@link VirtualColumn} to be coming from the base datasource. <br/>
   * An alternate approach would be to analyze these during the segmentmap
   * function creation.
   */
  public boolean isBaseColumn(String columnName)
  {
    for (String prefix : joinPrefixes) {
      if (JoinPrefixUtils.isPrefixedBy(columnName, prefix)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("rawtypes")
  public Query buildQueryWithBaseDataSource(DataSource newBaseDataSource)
  {
    return new ReplaceBaseDataSource(baseDataSource, newBaseDataSource).traverse(topQuery);
  }

  /**
   * Replaces the base datasource of the given query.
   */
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

  /**
   * Assembles the segment mapping function which should be applied to the input segments.
   */
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction()
  {
    DataSource topDataSource = getTopDataSource();
    return topDataSource.createSegmentMapFunction(topQuery);
  }

  /**
   * Returns the first datasource which is not collapsible by the topQuery.
   */
  private DataSource getTopDataSource()
  {
    Query<?> q = topQuery;
    while (q.mayCollapseQueryDataSource()) {
      q = ((QueryDataSource) q.getDataSource()).getQuery();
    }
    return q.getDataSource();
  }
}
