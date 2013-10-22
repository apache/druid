package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.metamx.druid.processing.FloatMetricSelector;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.metamx.druid.processing.ObjectColumnSelector;

import java.util.Comparator;

public class CardinalityAggregator implements Aggregator
{
    static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

    static Object combineValues(Object lhs, Object rhs)
    {
        try {
            return ((ICardinality) lhs).merge((ICardinality) rhs);
        }
        catch (CardinalityMergeException e) {
            return lhs;
        }
    }

    private final ObjectColumnSelector selector;
    private final String name;
    ICardinality card;

    public CardinalityAggregator(String name, ObjectColumnSelector selector)
    {
        this.name = name;
        this.selector = selector;
        this.card =  AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
    }

    public CardinalityAggregator(String name, ObjectColumnSelector selector, ICardinality card)
    {
        this.name = name;
        this.selector = selector;
        this.card =  card;
    }

    @Override
    public void aggregate()
    {
        card.offer(selector.get());
    }

    @Override
    public void reset()
    {
        this.card =  AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
    }

    @Override
    public Object get()
    {
        return card.cardinality();
    }

    @Override
    public float getFloat()
    {
        return (float) card.cardinality();
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Aggregator clone()
    {
        ICardinality card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
        try {
            card.merge(this.card);
        }
        catch (CardinalityMergeException e) {

        }
        return new CardinalityAggregator(this.name, this.selector, card);
    }

    @Override
    public void close()
    {
        this.card = null;
    }
}
