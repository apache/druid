package com.metamx.druid.query.order;

import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dyuan
 * Date: 2/11/13
 * Time: 6:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class DescendingOrderBySpec extends AbstractOrderBySpec implements OrderBySpec
{

  @JsonCreator
  public DescendingOrderBySpec
    (
      @JsonProperty("aggregations") List<String> aggregations
    ){
    super(aggregations);
  }


  @Override
  public Ordering<Row> getRowOrdering()
  {
    return getAscendingRowOrdering().reverse();
  }
}
