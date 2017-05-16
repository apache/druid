/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.query.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("scan")
public class ScanQuery extends BaseQuery<ScanResultValue>
{
  public static final String SCAN = "scan";
  public static final String RESULT_FORMAT_LIST = "list";
  public static final String RESULT_FORMAT_COMPACTED_LIST = "compactedList";
  public static final String RESULT_FORMAT_VALUE_VECTOR = "valueVector";

  private final String resultFormat;
  private final int batchSize;
  private final long limit;
  private final DimFilter dimFilter;
  private final List<String> columns;

  @JsonCreator
  public ScanQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("resultFormat") String resultFormat,
      @JsonProperty("batchSize") int batchSize,
      @JsonProperty("limit") long limit,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.resultFormat = resultFormat == null ? RESULT_FORMAT_LIST : resultFormat;
    this.batchSize = (batchSize == 0) ? 4096 * 5 : batchSize;
    this.limit = (limit == 0) ? Long.MAX_VALUE : limit;
    Preconditions.checkArgument(this.batchSize > 0, "batchSize must be greater than 0");
    Preconditions.checkArgument(this.limit > 0, "limit must be greater than 0");
    this.dimFilter = dimFilter;
    this.columns = columns;
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
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return SCAN;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
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

    ScanQuery that = (ScanQuery) o;

    if (batchSize != that.batchSize) {
      return false;
    }
    if (limit != that.limit) {
      return false;
    }
    if (resultFormat != null ? !resultFormat.equals(that.resultFormat) : that.resultFormat != null) {
      return false;
    }
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) {
      return false;
    }
    return columns != null ? columns.equals(that.columns) : that.columns == null;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (resultFormat != null ? resultFormat.hashCode() : 0);
    result = 31 * result + batchSize;
    result = 31 * result + (int) (limit ^ (limit >>> 32));
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ScanQuery{" +
        "dataSource='" + getDataSource() + '\'' +
        ", querySegmentSpec=" + getQuerySegmentSpec() +
        ", descending=" + isDescending() +
        ", resultFormat='" + resultFormat + '\'' +
        ", batchSize=" + batchSize +
        ", limit=" + limit +
        ", dimFilter=" + dimFilter +
        ", columns=" + columns +
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
   * @see io.druid.query.scan.ScanQuery
   */
  public static class ScanQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private Map<String, Object> context;
    private String resultFormat;
    private int batchSize;
    private long limit;
    private DimFilter dimFilter;
    private List<String> columns;

    public ScanQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      context = null;
      resultFormat = null;
      batchSize = 0;
      limit = 0;
      dimFilter = null;
      columns = Lists.newArrayList();
    }

    public ScanQuery build()
    {
      return new ScanQuery(
          dataSource,
          querySegmentSpec,
          resultFormat,
          batchSize,
          limit,
          dimFilter,
          columns,
          context
      );
    }

    public static ScanQueryBuilder copy(ScanQuery query)
    {
      return new ScanQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .resultFormat(query.getResultFormat())
          .batchSize(query.getBatchSize())
          .limit(query.getLimit())
          .filters(query.getFilter())
          .columns(query.getColumns())
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

    public ScanQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public ScanQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
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

    public ScanQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public ScanQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
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
  }

  public static ScanQueryBuilder newScanQueryBuilder()
  {
    return new ScanQueryBuilder();
  }
}
