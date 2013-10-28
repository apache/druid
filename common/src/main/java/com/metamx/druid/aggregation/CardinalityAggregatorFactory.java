package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
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
    private static final byte CACHE_TYPE_ID = 0x1;
    private static final ICardinality card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();

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

        log.info("New instance: name=%s, fieldName=%s", name, fieldName);

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
        log.info("Will create combining factory");
        return new CardinalityAggregatorFactory(name, name);
    }

    @Override
    public Object deserialize(Object object)
    {
        log.info("Deserialize: %s", object.getClass());
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
        return card.sizeof();
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
