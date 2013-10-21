package com.metamx.druid.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.metamx.druid.processing.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.List;

/**
 */
public class JonAggregatorFactory implements AggregatorFactory {
    private static final byte[] CACHE_KEY = new byte[]{0x0};
    private final String name;

    @JsonCreator
    public JonAggregatorFactory(
        @JsonProperty("name") String name
    )
    {
        Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
        this.name = name;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory)
    {
        return new JonAggregator(name);
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
    {
        return new JonBufferAggregator();
    }

    @Override
    public Comparator getComparator()
    {
        return JonAggregator.COMPARATOR;
    }

    @Override
    public Object combine(Object lhs, Object rhs)
    {
        return JonAggregator.combineValues(lhs, rhs);
    }

    @Override
    public AggregatorFactory getCombiningFactory()
    {
        return new LongSumAggregatorFactory(name, name);
    }

    @Override
    public Object deserialize(Object object)
    {
        return object;
    }

    @Override
    public Object finalizeComputation(Object object)
    {
        return object;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    public List<String> requiredFields()
    {
        return ImmutableList.of();
    }

    @Override
    public byte[] getCacheKey()
    {
        return CACHE_KEY;
    }

    @Override
    public String getTypeName() {
        return "float";
    }

    @Override
    public int getMaxIntermediateSize()
    {
        return Longs.BYTES;
    }

    @Override
    public Object getAggregatorStartValue()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return "JonAggregatorFactor{" +
                "name='" + name + "'" +
                "}";
    }
}
