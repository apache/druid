package com.metamx.druid.aggregation;

import com.google.common.collect.Lists;
import com.metamx.druid.processing.FloatMetricSelector;

import java.util.List;

public class JavaScriptAggregator implements Aggregator
{
  static interface ScriptAggregator
  {
    public double aggregate(double current, FloatMetricSelector[] selectorList);

    public double combine(double a, double b);

    public double reset();
  }

  private final String name;
  private final FloatMetricSelector[] selectorList;
  private final ScriptAggregator script;

  private volatile double current;

  public JavaScriptAggregator(String name, List<FloatMetricSelector> selectorList, ScriptAggregator script)
  {
    this.name = name;
    this.selectorList = Lists.newArrayList(selectorList).toArray(new FloatMetricSelector[]{});
    this.script = script;

    this.current = script.reset();
  }

  @Override
  public void aggregate()
  {
    current = script.aggregate(current, selectorList);
  }

  @Override
  public void reset()
  {
    current = script.reset();
  }

  @Override
  public Object get()
  {
    return current;
  }

  @Override
  public float getFloat()
  {
    return (float) current;
  }

  @Override
  public String getName()
  {
    return name;
  }
}
