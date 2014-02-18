/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
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
  public Firehose connect(InputRowParser parser) throws IOException
  {
    return new PredicateFirehose(
        delegate.connect(parser),
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

  @Override
  public InputRowParser getParser()
  {
    return delegate.getParser();
  }
}
