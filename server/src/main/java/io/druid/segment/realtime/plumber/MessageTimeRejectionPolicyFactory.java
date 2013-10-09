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

package io.druid.segment.realtime.plumber;

import org.joda.time.DateTime;
import org.joda.time.Period;

public class MessageTimeRejectionPolicyFactory implements RejectionPolicyFactory
{
  @Override
  public RejectionPolicy create(final Period windowPeriod)
  {
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    return new RejectionPolicy()
    {
      private volatile long maxTimestamp = Long.MIN_VALUE;

      @Override
      public DateTime getCurrMaxTime()
      {
        return new DateTime(maxTimestamp);
      }

      @Override
      public boolean accept(long timestamp)
      {
        maxTimestamp = Math.max(maxTimestamp, timestamp);

        return timestamp >= (maxTimestamp - windowMillis);
      }

      @Override
      public String toString()
      {
        return String.format("messageTime-%s", windowPeriod);
      }
    };
  }
}

