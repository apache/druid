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
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Map;

public class WindowOperatorQuery extends BaseQuery<RowsAndColumns>
{
  private final List<String> partitionDimensions;
  private final Processor processor;
  private final RowSignature rowSignature;

  @JsonCreator
  public WindowOperatorQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("processor") Processor processor,
      @JsonProperty("partitionColumns") List<String> partitionDimensions,
      @JsonProperty("outputSignature") RowSignature rowSignature
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.rowSignature = rowSignature;
    if (! (dataSource instanceof QueryDataSource || dataSource instanceof InlineDataSource)) {
      throw new IAE("WindowOperatorQuery must run on top of a query or inline data source, got [%s]", dataSource);
    }
    this.partitionDimensions = partitionDimensions;
    this.processor = processor;
  }

  @JsonProperty("partitionColumns")
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @JsonProperty("processor")
  public Processor getProcessor()
  {
    return processor;
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
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return "operator";
  }

  @Override
  public Query<RowsAndColumns> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new WindowOperatorQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        computeOverriddenContext(getContext(), contextOverride),
        processor,
        partitionDimensions,
        rowSignature
    );
  }

  @Override
  public Query<RowsAndColumns> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new WindowOperatorQuery(
        getDataSource(),
        spec,
        getContext(),
        processor,
        partitionDimensions,
        rowSignature
    );
  }

  @Override
  public Query<RowsAndColumns> withDataSource(DataSource dataSource)
  {
    return new WindowOperatorQuery(
        dataSource,
        getQuerySegmentSpec(),
        getContext(),
        processor,
        partitionDimensions,
        rowSignature
    );
  }
}
