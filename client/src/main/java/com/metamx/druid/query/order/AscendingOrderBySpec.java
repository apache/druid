package com.metamx.druid.query.order;

import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * This "order by" spec creates an ascending sorting order for the given aggregations.
 */
public class AscendingOrderBySpec extends AbstractOrderBySpec implements OrderBySpec
{
  @JsonCreator
  public AscendingOrderBySpec
  (
    @JsonProperty("aggregations") List<String> aggregations
  )
  {
    super(aggregations);
  }

  @Override
  public Ordering<Row> getRowOrdering()
  {
    return getAscendingRowOrdering();
  }
}
