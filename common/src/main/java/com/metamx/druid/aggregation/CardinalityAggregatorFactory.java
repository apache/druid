package com.metamx.druid.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.metamx.druid.processing.ColumnSelectorFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class CardinalityAggregatorFactory implements AggregatorFactory
{
    private static final byte[] CACHE_KEY = new byte[]{0x0};

    private final String fieldName;
    private final String name;

    @JsonCreator
    public CardinalityAggregatorFactory(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") final String fieldName
    )
    {
        Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
        Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory)
    {
        return new CardinalityAggregator(name, metricFactory.makeObjectColumnSelector(fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
    {
        return new CardinalityBufferAggregator(metricFactory.makeObjectColumnSelector(fieldName));
    }

    @Override
    public Comparator getComparator()
    {
        return CardinalityAggregator.COMPARATOR;
    }

    @Override
    public Object combine(Object lhs, Object rhs)
    {
        return CardinalityAggregator.combineValues(lhs, rhs);
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

    @JsonProperty
    public String getFieldName()
    {
        return fieldName;
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
        return Arrays.asList(fieldName);
    }

    @Override
    public byte[] getCacheKey()
    {
        return CACHE_KEY;
    }

    @Override
    public String getTypeName() {
        return "string";
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
        return "CardinalityAggregatorFactory{" +
            "fieldName='" + fieldName + '\'' +
            ", name='" + name + '\'' +
            '}';
    }
}
