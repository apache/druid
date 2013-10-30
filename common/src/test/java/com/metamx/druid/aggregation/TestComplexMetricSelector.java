package com.metamx.druid.aggregation;

import com.metamx.druid.processing.ComplexMetricSelector;

public class TestComplexMetricSelector<T> implements ComplexMetricSelector<T> {

    private final T[] metrics;
    private final Class<T> clazz;

    private int index = 0;

    public TestComplexMetricSelector(Class<T> clazz, T[] metrics)
    {
        this.clazz = clazz;
        this.metrics = metrics;
    }

    @Override
    public Class<T> classOfObject() {
        return clazz;
    }

    @Override
    public T get() {
        return metrics[index];
    }

    public void increment()
    {
        ++index;
    }
}
