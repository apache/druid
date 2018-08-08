package io.druid.query.aggregation.datasketches.tuple;

import org.junit.Assert;
import org.junit.Test;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.TestObjectColumnSelector;

public class ArrayOfDoublesSketchAggregatorFactoryTest
{

  @Test
  public void makeAggregateCombiner() {
    AggregatorFactory aggregatorFactory = new ArrayOfDoublesSketchAggregatorFactory("", "", null, null, null);
    AggregatorFactory combiningFactory = aggregatorFactory.getCombiningFactory();
    AggregateCombiner<ArrayOfDoublesSketch> combiner = combiningFactory.makeAggregateCombiner();

    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update("a", new double[] {1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update("b", new double[] {1});
    sketch2.update("c", new double[] {1});

    TestObjectColumnSelector<ArrayOfDoublesSketch> selector = new TestObjectColumnSelector<ArrayOfDoublesSketch>(new ArrayOfDoublesSketch[] {sketch1, sketch2});

    combiner.reset(selector);
    Assert.assertEquals(1, combiner.getObject().getEstimate(), 0);

    selector.increment();
    combiner.fold(selector);
    Assert.assertEquals(3, combiner.getObject().getEstimate(), 0);
  }

}
