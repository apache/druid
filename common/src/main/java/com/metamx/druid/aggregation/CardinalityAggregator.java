package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.primitives.Longs;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ComplexMetricSelector;

import java.io.IOException;
import java.util.Comparator;

public class CardinalityAggregator implements Aggregator
{
    static final Comparator COMPARATOR = new Comparator()
    {
        @Override
        public int compare(Object o, Object o1)
        {
        return Longs.compare(((ICardinality) o).cardinality(), ((ICardinality) o1).cardinality());
        }
    };

    static Object combineValues(Object lhs, Object rhs)
    {
        try {
            return ((ICardinality) lhs).merge((ICardinality) rhs);
        }
        catch (CardinalityMergeException e) {
            return lhs;
        }
    }

    private static final Logger log = new Logger(CardinalityAggregator.class);

    private final ComplexMetricSelector<ICardinality> selector;
    private final String name;
    ICardinality card;

    public CardinalityAggregator(String name, ComplexMetricSelector<ICardinality> selector)
    {
        this.name = name;
        this.selector = selector;
        this.card =  AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
    }

    public CardinalityAggregator(String name, ComplexMetricSelector<ICardinality> selector, ICardinality card)
    {
        this.name = name;
        this.selector = selector;
        this.card =  card;
    }

    @Override
    public void aggregate()
    {
        ICardinality valueToAgg = selector.get();
        try {
            ICardinality mergedCardinality = card.merge(valueToAgg);
            card = mergedCardinality;
        }
        catch (CardinalityMergeException e) {
            throw new RuntimeException("Failed to aggregate: "+e);
        }
    }

    @Override
    public void reset()
    {
        card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
    }

    @Override
    public Object get()
    {
        return card;
    }

    @Override
    public float getFloat()
    {
        throw new UnsupportedOperationException("CardinalityAggregator does not support getFloat()");
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Aggregator clone()
    {
        try {
            ICardinality card = new AdaptiveCounting(this.card.getBytes());
            return new CardinalityAggregator(this.name, this.selector, card);
        }
        catch (IOException e) {

        }

        return null;
    }

    @Override
    public void close()
    {
        this.card = null;
    }
}
