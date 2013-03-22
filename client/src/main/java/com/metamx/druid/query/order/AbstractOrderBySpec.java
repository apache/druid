package com.metamx.druid.query.order;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dyuan
 * Date: 2/11/13
 * Time: 6:46 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractOrderBySpec implements OrderBySpec
{
  // A list of names of either aggregations or post aggregations.
  private final List<String> aggregations;

  public AbstractOrderBySpec(List<String> aggregations)
  {
    Preconditions.checkArgument(aggregations != null && !aggregations.isEmpty(), "There should be at least one aggregation name");

    this.aggregations = ImmutableList.copyOf(aggregations);
  }

  @Override
  public List<String> getAggregations()
  {
    return aggregations;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Ordering<Row> getAscendingRowOrdering()
  {
    List<String> aggregations = getAggregations();
    Iterable<Comparator<Row>> comparators = Iterables.transform(getAggregations(), new Function<String, Comparator<Row>>()
    {
      @Override
      public Comparator<Row> apply(@Nullable final String aggName)
      {
        return new Comparator<Row>(){

          @Override
          public int compare(Row r1, Row r2)
          {
            return Float.compare(r1.getFloatMetric(aggName), r2.getFloatMetric(aggName));
          }
        };
      }
    });

    return Ordering.compound(comparators);
  }
}
