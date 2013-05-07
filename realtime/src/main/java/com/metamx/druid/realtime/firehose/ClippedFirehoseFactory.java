package com.metamx.druid.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.metamx.druid.input.InputRow;
import org.joda.time.Interval;

import java.io.IOException;

/**
 * Creates firehoses clipped to a particular time interval. Useful for enforcing min time, max time, and time windows.
 */
public class ClippedFirehoseFactory implements FirehoseFactory
{
  private final FirehoseFactory delegate;
  private final Interval interval;

  @JsonCreator
  public ClippedFirehoseFactory(
      @JsonProperty("delegate") FirehoseFactory delegate,
      @JsonProperty("interval") Interval interval
  )
  {
    this.delegate = delegate;
    this.interval = interval;
  }

  @JsonProperty
  public FirehoseFactory getDelegate()
  {
    return delegate;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public Firehose connect() throws IOException
  {
    return new PredicateFirehose(
        delegate.connect(),
        new Predicate<InputRow>()
        {
          @Override
          public boolean apply(InputRow input)
          {
            return interval.contains(input.getTimestampFromEpoch());
          }
        }
    );
  }
}
