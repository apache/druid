package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.metamx.druid.processing.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class CardinalityBufferAggregator implements BufferAggregator
{
    private final ObjectColumnSelector selector;
    ICardinality card;

    public CardinalityBufferAggregator(ObjectColumnSelector selector)
    {
        this.selector = selector;
        this.card =  AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
        buf.putLong(position, card.cardinality());
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
        card.offer(selector.get());
        buf.putLong(position, card.cardinality());
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
        return buf.getLong(position);
    }

    @Override
    public float getFloat(ByteBuffer buf, int position)
    {
        return buf.getLong(position);
    }

    @Override
    public void close()
    {
        this.card = null;
    }
}
