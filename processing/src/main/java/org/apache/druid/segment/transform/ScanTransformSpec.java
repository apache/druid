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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumn;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link BaseTransformSpec} that processes input rows through an embedded {@link ScanQuery}.
 * The scan query can include unnest data sources, virtual columns, and filters.
 *
 * <p>Example JSON:
 * <pre>{@code
 * "transformSpec": {
 *   "type": "scan",
 *   "query": {
 *     "queryType": "scan",
 *     "dataSource": {
 *       "type": "unnest",
 *       "base": { "type": "table", "name": "__input__" },
 *       "virtualColumn": { "type": "expression", "name": "tag", "expression": "\"tags\"", "outputType": "STRING" }
 *     },
 *     "intervals": { "type": "intervals", "intervals": ["-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"] },
 *     "resultFormat": "list"
 *   }
 * }
 * }</pre>
 */
@JsonTypeName("scan")
public class ScanTransformSpec implements BaseTransformSpec
{
  private final ScanQuery query;

  @JsonCreator
  public ScanTransformSpec(@JsonProperty("query") final ScanQuery query)
  {
    this.query = query;
  }

  @JsonProperty
  public ScanQuery getQuery()
  {
    return query;
  }

  @Override
  public BaseTransformer toTransformer()
  {
    return new ScanTransformer(query);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    final Set<String> columns = new HashSet<>();
    collectRequiredColumns(query.getDataSource(), columns);
    for (final VirtualColumn vc : query.getVirtualColumns().getVirtualColumns()) {
      columns.addAll(vc.requiredColumns());
    }
    if (query.getFilter() != null) {
      columns.addAll(query.getFilter().getRequiredColumns());
    }
    return columns;
  }

  private static void collectRequiredColumns(final DataSource dataSource, final Set<String> columns)
  {
    if (dataSource instanceof UnnestDataSource) {
      final UnnestDataSource unnest = (UnnestDataSource) dataSource;
      columns.addAll(unnest.getVirtualColumn().requiredColumns());
      collectRequiredColumns(unnest.getBase(), columns);
    }
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ScanTransformSpec that = (ScanTransformSpec) o;
    return Objects.equals(query, that.query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query);
  }

  @Override
  public String toString()
  {
    return "ScanTransformSpec{query=" + query + '}';
  }
}
