package com.metamx.druid.query.order;

import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dyuan
 * Date: 2/11/13
 * Time: 6:22 PM
 * To change this template use File | Settings | File Templates.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = AscendingOrderBySpec.class)
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "asc", value = AscendingOrderBySpec.class),
        @JsonSubTypes.Type(name = "desc", value = DescendingOrderBySpec.class)
})
public interface OrderBySpec
{
  public List<String> getAggregations();
  public Ordering<Row> getRowOrdering();
}
