package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class CardinalityAggregatorFactory implements AggregatorFactory
{
    private static final Logger log = new Logger(CardinalityAggregatorFactory.class);
    private static final byte CACHE_TYPE_ID = 0x8;

    private final String fieldName;
    private final String name;

    @JsonCreator
    public CardinalityAggregatorFactory(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") final String fieldName
    )
    {
        Preconditions.checkArgument(fieldName != null && fieldName.length() > 0, "Must have a valid, non-null aggregator name");
        Preconditions.checkArgument(fieldName != null && fieldName.length() > 0, "Must have a valid, non-null fieldName");

        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory)
    {
        return new CardinalityAggregator(name, metricFactory.makeComplexMetricSelector(fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
    {
        return new CardinalityBufferAggregator(metricFactory.makeComplexMetricSelector(fieldName));
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
        return new CardinalityAggregatorFactory(name, name);
    }

    @Override
    public Object deserialize(Object object)
    {
        if (object instanceof byte[]) {
            return new AdaptiveCounting((byte[])object);
        }
        else if (object instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer)object;
            int size = buf.remaining();
            byte[] bytes = new byte[size];
            buf.get(bytes);
            return new AdaptiveCounting(bytes);
        }
        return object;
    }

    @Override
    public Object finalizeComputation(Object object)
    {
        return ((ICardinality)object).cardinality();
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
        byte[] fieldNameBytes = fieldName.getBytes();
        return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
    }

    @Override
    public String getTypeName() {
        return "cardinality";
    }

    @Override
    public int getMaxIntermediateSize()
    {
        return 65536;
    }

    @Override
    public Object getAggregatorStartValue()
    {
        return AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
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
