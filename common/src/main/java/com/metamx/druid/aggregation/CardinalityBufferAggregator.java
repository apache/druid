package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ComplexMetricSelector;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CardinalityBufferAggregator implements BufferAggregator
{
    private static final Logger log = new Logger(CardinalityBufferAggregator.class);

    private final ComplexMetricSelector<ICardinality> selector;
    ICardinality card;

    public CardinalityBufferAggregator(ComplexMetricSelector<ICardinality> selector)
    {
        log.info("New instance: selectorClass=%s", selector.getClass());

        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
        this.card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
        try {
            byte[] bytes = card.getBytes();
            int size = card.sizeof();
            for (int i = 0; i < size; i++) {
                buf.put(position + i, bytes[i]);
            }
        }
        catch (IOException e) {

        }
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
        int size = card.sizeof();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = buf.get(position + i);
        }
        ICardinality cardinalityCounter = new AdaptiveCounting(bytes);
        ICardinality valueToAgg = selector.get();
        try {
            cardinalityCounter = cardinalityCounter.merge(valueToAgg);
            bytes = cardinalityCounter.getBytes();
            for (int i = 0; i < size; i++) {
                buf.put(position + i, bytes[i]);
            }
        }
        catch (CardinalityMergeException e) {

        }
        catch (IOException e) {

        }
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
        int size = card.sizeof();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = buf.get(position + i);
        }
        ICardinality cardinalityCounter = new AdaptiveCounting(bytes);
        return cardinalityCounter;
    }

    @Override
    public float getFloat(ByteBuffer buf, int position)
    {
        throw new UnsupportedOperationException("CardinalityBufferAggregator does not support getFloat()");
    }

    @Override
    public void close()
    {
        this.card = null;
    }
}
