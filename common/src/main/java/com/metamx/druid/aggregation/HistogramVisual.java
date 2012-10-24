package com.metamx.druid.aggregation;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;

public class HistogramVisual
{
  @JsonProperty final public double[] breaks;
  @JsonProperty final public double[] counts;
  @JsonProperty final double min;
  @JsonProperty final double max;

  @JsonCreator
  public HistogramVisual(
      @JsonProperty double[] breaks,
      @JsonProperty double[] counts,
      @JsonProperty double min,
      @JsonProperty double max
  )
  {
    Preconditions.checkArgument(breaks != null, "breaks must not be null");
    Preconditions.checkArgument(counts != null, "counts must not be null");
    Preconditions.checkArgument(breaks.length == counts.length + 1, "breaks.length must be counts.length + 1");

    this.breaks = breaks;
    this.counts = counts;
    this.min = min;
    this.max = max;
  }

  public HistogramVisual(
        float[] breaks,
        float[] counts,
        float min,
        float max
  )
  {
    Preconditions.checkArgument(breaks != null, "breaks must not be null");
    Preconditions.checkArgument(counts != null, "counts must not be null");
    Preconditions.checkArgument(breaks.length == counts.length + 1, "breaks.length must be counts.length + 1");

    this.breaks = new double[breaks.length];
    this.counts = new double[counts.length];
    for(int i = 0; i < breaks.length; ++i) this.breaks[i] = breaks[i];
    for(int i = 0; i < counts.length; ++i) this.counts[i] = counts[i];
    this.min = min;
    this.max = max;
  }

  @Override
  public String toString()
  {
    return "HistogramVisual{" +
           "counts=" + Arrays.toString(counts) +
           ", breaks=" + Arrays.toString(breaks) +
           ", min=" + min +
           ", max=" + max +
           '}';
  }
}
