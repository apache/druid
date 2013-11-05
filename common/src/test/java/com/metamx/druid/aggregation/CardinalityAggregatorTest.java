package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

public class CardinalityAggregatorTest {
    private void aggregate(TestComplexMetricSelector<ICardinality> selector, CardinalityAggregator agg)
    {
        agg.aggregate();
        selector.increment();
    }

    @Test
    public void testAggregate()
    {
        ICardinality card1 = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
        ICardinality card2 = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();

        card1.offer("1");
        card1.offer("2");
        card1.offer("3");

        card2.offer("3");
        card2.offer("4");
        card2.offer("5");

        final TestComplexMetricSelector<ICardinality> selector = new TestComplexMetricSelector<ICardinality>(ICardinality.class, new ICardinality[]{card1, card2});
        CardinalityAggregator agg = new CardinalityAggregator("billy", selector);

        Assert.assertEquals("billy", agg.getName());

        Assert.assertEquals(0, ((ICardinality)agg.get()).cardinality());
        Assert.assertEquals(0, ((ICardinality)agg.get()).cardinality());
        Assert.assertEquals(0, ((ICardinality)agg.get()).cardinality());
        aggregate(selector, agg);
        Assert.assertEquals(3, ((ICardinality)agg.get()).cardinality());
        Assert.assertEquals(3, ((ICardinality)agg.get()).cardinality());
        Assert.assertEquals(3, ((ICardinality)agg.get()).cardinality());
        aggregate(selector, agg);
        Assert.assertEquals(5, ((ICardinality)agg.get()).cardinality());
        Assert.assertEquals(5, ((ICardinality)agg.get()).cardinality());
        Assert.assertEquals(5, ((ICardinality)agg.get()).cardinality());
    }

    @Test
    public void testComparator()
    {
        ICardinality card1 = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();

        card1.offer("1");
        card1.offer("2");
        card1.offer("3");

        final TestComplexMetricSelector<ICardinality> selector = new TestComplexMetricSelector<ICardinality>(ICardinality.class, new ICardinality[]{card1});
        CardinalityAggregator agg = new CardinalityAggregator("billy", selector);

        Assert.assertEquals("billy", agg.getName());

        Object first = agg.get();
        agg.aggregate();

        Comparator comp = new CardinalityAggregatorFactory("null", "null").getComparator();

        Assert.assertEquals(-1, comp.compare(first, agg.get()));
        Assert.assertEquals(0, comp.compare(first, first));
        Assert.assertEquals(0, comp.compare(agg.get(), agg.get()));
        Assert.assertEquals(1, comp.compare(agg.get(), first));
    }
}
