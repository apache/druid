package com.metamx.druid.aggregation;


import java.util.Comparator;

public class JonAggregator implements Aggregator {
    static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

    static Object combineValues(Object lhs, Object rhs)
    {
        return ((Number) lhs).longValue() + ((Number) rhs).longValue();
    }

    long count = 0;
    private final String name;

    public JonAggregator(String name)
    {
        this.name = name;
    }

    @Override
    public void aggregate()
    {
        ++count;
        ++count;
    }

    @Override
    public void reset()
    {
        count = 0;
    }

    @Override
    public Object get()
    {
        return count;
    }

    @Override
    public float getFloat()
    {
        return (float) count;
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public Aggregator clone()
    {
        return new JonAggregator(name);
    }

    @Override
    public void close()
    {
        // nothing to clean up
    }
}
