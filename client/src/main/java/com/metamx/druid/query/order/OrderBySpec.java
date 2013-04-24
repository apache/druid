package com.metamx.druid.query.order;

import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.List;

/**
 * This spec is similar to "order by" clause in SQL. It sorts the result of a "group by" query in either
 * ascending or descending order. It supports ordering by multiple aggregations too.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = AscendingOrderBySpec.class)
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "asc", value = AscendingOrderBySpec.class),
        @JsonSubTypes.Type(name = "desc", value = DescendingOrderBySpec.class)
})
public interface OrderBySpec
{
  /**
   * @return the list of aggregation names. Query results will be sorted by
   * each aggregation in this list.
   *
   */
  public List <String> getAggregations();

  /**
   * @return the ordering based on the given list of aggregations
   * @see Ordering
   */
  public Ordering<Row> getRowOrdering();
}
