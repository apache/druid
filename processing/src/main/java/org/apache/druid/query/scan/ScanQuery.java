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
package org.apache.druid.query.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ScanQuery extends BaseQuery<ScanResultValue>
{
  public static final String RESULT_FORMAT_LIST = "list";
  public static final String RESULT_FORMAT_COMPACTED_LIST = "compactedList";
  public static final String RESULT_FORMAT_VALUE_VECTOR = "valueVector";

  private final VirtualColumns virtualColumns;
  private final String resultFormat;
  private final int batchSize;
  private final long limit;
  private final DimFilter dimFilter;
  private final List<String> columns;
  private final Boolean legacy;

  @JsonCreator
  public ScanQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("resultFormat") String resultFormat,
      @JsonProperty("batchSize") int batchSize,
      @JsonProperty("limit") long limit,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("legacy") Boolean legacy,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.resultFormat = resultFormat == null ? RESULT_FORMAT_LIST : resultFormat;
    this.batchSize = (batchSize == 0) ? 4096 * 5 : batchSize;
    this.limit = (limit == 0) ? Long.MAX_VALUE : limit;
    Preconditions.checkArgument(this.batchSize > 0, "batchSize must be greater than 0");
    Preconditions.checkArgument(this.limit > 0, "limit must be greater than 0");
    this.dimFilter = dimFilter;
    this.columns = columns;
    this.legacy = legacy;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public String getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  public int getBatchSize()
  {
    return batchSize;
  }

  @JsonProperty
  public long getLimit()
  {
    return limit;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  @JsonProperty
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return SCAN;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  /**
   * Compatibility mode with the legacy scan-query extension.
   */
  @JsonProperty
  public Boolean isLegacy()
  {
    return legacy;
  }

  public ScanQuery withNonNullLegacy(final ScanQueryConfig scanQueryConfig)
  {
    return ScanQueryBuilder.copy(this).legacy(legacy != null ? legacy : scanQueryConfig.isLegacy()).build();
  }

  @Override
  public Query<ScanResultValue> withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return ScanQueryBuilder.copy(this).intervals(querySegmentSpec).build();
  }

  @Override
  public Query<ScanResultValue> withDataSource(DataSource dataSource)
  {
    return ScanQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  @Override
  public Query<ScanResultValue> withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return ScanQueryBuilder.copy(this).context(computeOverriddenContext(getContext(), contextOverrides)).build();
  }

  public ScanQuery withDimFilter(DimFilter dimFilter)
  {
    return ScanQueryBuilder.copy(this).filters(dimFilter).build();
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
    if (!super.equals(o)) {
      return false;
    }
    final ScanQuery scanQuery = (ScanQuery) o;
    return batchSize == scanQuery.batchSize &&
           limit == scanQuery.limit &&
           legacy == scanQuery.legacy &&
           Objects.equals(virtualColumns, scanQuery.virtualColumns) &&
           Objects.equals(resultFormat, scanQuery.resultFormat) &&
           Objects.equals(dimFilter, scanQuery.dimFilter) &&
           Objects.equals(columns, scanQuery.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), virtualColumns, resultFormat, batchSize, limit, dimFilter, columns, legacy);
  }

  @Override
  public String toString()
  {
    return "ScanQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", virtualColumns=" + getVirtualColumns() +
           ", resultFormat='" + resultFormat + '\'' +
           ", batchSize=" + batchSize +
           ", limit=" + limit +
           ", dimFilter=" + dimFilter +
           ", columns=" + columns +
           ", legacy=" + legacy +
           '}';
  }

  /**
   * A Builder for ScanQuery.
   * <p/>
   * Required: dataSource(), intervals() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   ScanQuery query = new ScanQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .interval("2010/2013")
   *                                  .build();
   * </code></pre>
   *
   * @see ScanQuery
   */
  public static class ScanQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private VirtualColumns virtualColumns;
    private Map<String, Object> context;
    private String resultFormat;
    private int batchSize;
    private long limit;
    private DimFilter dimFilter;
    private List<String> columns;
    private Boolean legacy;

    public ScanQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      virtualColumns = null;
      context = null;
      resultFormat = null;
      batchSize = 0;
      limit = 0;
      dimFilter = null;
      columns = Lists.newArrayList();
      legacy = null;
    }

    public ScanQuery build()
    {
      return new ScanQuery(
          dataSource,
          querySegmentSpec,
          virtualColumns,
          resultFormat,
          batchSize,
          limit,
          dimFilter,
          columns,
          legacy,
          context
      );
    }

    public static ScanQueryBuilder copy(ScanQuery query)
    {
      return new ScanQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .virtualColumns(query.getVirtualColumns())
          .resultFormat(query.getResultFormat())
          .batchSize(query.getBatchSize())
          .limit(query.getLimit())
          .filters(query.getFilter())
          .columns(query.getColumns())
          .legacy(query.isLegacy())
          .context(query.getContext());
    }

    public ScanQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public ScanQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public ScanQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public ScanQueryBuilder virtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public ScanQueryBuilder virtualColumns(VirtualColumn... virtualColumns)
    {
      return virtualColumns(VirtualColumns.create(Arrays.asList(virtualColumns)));
    }

    public ScanQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }

    public ScanQueryBuilder resultFormat(String r)
    {
      resultFormat = r;
      return this;
    }

    public ScanQueryBuilder batchSize(int b)
    {
      batchSize = b;
      return this;
    }

    public ScanQueryBuilder limit(long l)
    {
      limit = l;
      return this;
    }

    public ScanQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public ScanQueryBuilder columns(List<String> c)
    {
      columns = c;
      return this;
    }

    public ScanQueryBuilder columns(String... c)
    {
      columns = Arrays.asList(c);
      return this;
    }

    public ScanQueryBuilder legacy(Boolean legacy)
    {
      this.legacy = legacy;
      return this;
    }
  }

  public static ScanQueryBuilder newScanQueryBuilder()
  {
    return new ScanQueryBuilder();
  }
}
