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
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Visitor for traversing the execution vertices.
 */
abstract class ExecutionVertexShuttle
{
  /**
   * Execution vertex node.
   *
   * Can be a {@link Query} or a {@link DataSource}.
   */
  static class EVNode
  {
    final DataSource dataSource;
    final Query<?> query;
    final Integer index;

    EVNode(DataSource dataSource, Integer index)
    {
      this.dataSource = dataSource;
      this.query = null;
      this.index = index;
    }

    EVNode(Query<?> query, Integer index)
    {
      this.dataSource = null;
      this.query = query;
      this.index = index;
    }

    public boolean isQuery()
    {
      return query != null;
    }

    public Query<?> getQuery()
    {
      Preconditions.checkNotNull(query, "query is null!");
      return query;
    }
  }
  protected Stack<ExecutionVertexShuttle.EVNode> parents = new Stack<>();

  protected final Query<?> traverse(Query<?> query)
  {
    try {
      parents.push(new EVNode(query, null));
      if (!mayTraverseQuery(query)) {
        return query;
      }
      if (query instanceof BaseQuery<?>) {
        BaseQuery<?> baseQuery = (BaseQuery<?>) query;
        DataSource oldDataSource = baseQuery.getDataSource();
        DataSource newDataSource = traverse(oldDataSource, null);
        if (oldDataSource != newDataSource) {
          query = baseQuery.withDataSource(newDataSource);
        }
      } else {
        throw DruidException.defensive("Can't traverse a query[%s]!", query);
      }
      return visitQuery(query);
    }
    finally {
      parents.pop();
    }
  }

  protected final DataSource traverse(DataSource dataSource, Integer index)
  {
    try {
      parents.push(new EVNode(dataSource, index));
      boolean traverse = mayTraverseDataSource(parents.peek());
      if (dataSource instanceof QueryDataSource) {
        QueryDataSource queryDataSource = (QueryDataSource) dataSource;
        if (traverse) {
          Query<?> oldQuery = queryDataSource.getQuery();
          Query<?> newQuery = traverse(oldQuery);
          if (oldQuery != newQuery) {
            dataSource = new QueryDataSource(newQuery);
          }
        }
        return visit(dataSource, !traverse);
      } else {
        List<DataSource> children = dataSource.getChildren();
        List<DataSource> newChildren = new ArrayList<>();
        boolean changed = false;
        if (traverse) {
          for (int i = 0; i < children.size(); i++) {
            DataSource oldDS = children.get(i);
            DataSource newDS = traverse(oldDS, i);
            newChildren.add(newDS);
            changed |= (oldDS != newDS);
          }
        }
        DataSource newDataSource = changed ? dataSource.withChildren(newChildren) : dataSource;
        return visit(newDataSource, !traverse);

      }
    }
    finally {
      parents.pop();
    }
  }

  protected abstract boolean mayTraverseQuery(Query<?> query);

  protected abstract boolean mayTraverseDataSource(ExecutionVertexShuttle.EVNode evNode);

  protected abstract DataSource visit(DataSource dataSource, boolean leaf);

  protected abstract Query<?> visitQuery(Query<?> query);
}
