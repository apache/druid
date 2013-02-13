/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.metamx.druid.QueryGranularity;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.ReadableInterval;
import org.joda.time.Weeks;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 */
public enum IndexGranularity
{
  MINUTE
      {
        DateTimeFormatter format = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm");
        private final int MILLIS_IN = 60 * 1000;

        @Override
        public String toPath(DateTime time)
        {
          return format.print(time);
        }

        @Override
        public DateTime increment(DateTime time)
        {
          return time.plus(Minutes.ONE);
        }

        @Override
        public DateTime increment(DateTime time, int count)
        {
          return time.plus(Minutes.minutes(count));
        }

        @Override
        public long increment(long timeMillis)
        {
          return timeMillis + MILLIS_IN;
        }

        @Override
        public DateTime decrement(DateTime time)
        {
          return time.minus(Minutes.ONE);
        }

        @Override
        public DateTime decrement(DateTime time, int count)
        {
          return time.minus(Minutes.minutes(count));
        }

        @Override
        public long decrement(long timeMillis)
        {
          return timeMillis - MILLIS_IN;
        }

        @Override
        public long truncate(long timeMillis)
        {
          return QueryGranularity.MINUTE.truncate(timeMillis);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfSecond(0);
          mutableDateTime.setSecondOfMinute(0);

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Minutes.minutesIn(interval).getMinutes();
        }
      },
  HOUR
      {
        DateTimeFormatter format = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'H'=HH");
        private final int MILLIS_IN = 60 * 60 * 1000;

        @Override
        public String toPath(DateTime time)
        {
          return format.print(time);
        }

        @Override
        public DateTime increment(DateTime time)
        {
          return time.plus(Hours.ONE);
        }

        @Override
        public DateTime increment(DateTime time, int count)
        {
          return time.plus(Hours.hours(count));
        }

        @Override
        public long increment(long timeMillis)
        {
          return timeMillis + MILLIS_IN;
        }

        @Override
        public DateTime decrement(DateTime time)
        {
          return time.minus(Hours.ONE);
        }

        @Override
        public DateTime decrement(DateTime time, int count)
        {
          return time.minus(Hours.hours(count));
        }

        @Override
        public long decrement(long timeMillis)
        {
          return timeMillis - MILLIS_IN;
        }

        @Override
        public long truncate(long timeMillis)
        {
          return QueryGranularity.HOUR.truncate(timeMillis);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfSecond(0);
          mutableDateTime.setSecondOfMinute(0);
          mutableDateTime.setMinuteOfHour(0);

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Hours.hoursIn(interval).getHours();
        }
      },
  DAY
      {
        DateTimeFormatter format = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd");
        private final int MILLIS_IN = 24 * 60 * 60 * 1000;

        @Override
        public String toPath(DateTime time)
        {
          return format.print(time);
        }

        @Override
        public DateTime increment(DateTime time)
        {
          return time.plus(Days.ONE);
        }

        @Override
        public DateTime increment(DateTime time, int count)
        {
          return time.plus(Days.days(count));
        }

        @Override
        public long increment(long timeMillis)
        {
          return timeMillis - MILLIS_IN;
        }

        @Override
        public DateTime decrement(DateTime time)
        {
          return time.minus(Days.ONE);
        }

        @Override
        public DateTime decrement(DateTime time, int count)
        {
          return time.minus(Days.days(count));
        }

        @Override
        public long decrement(long timeMillis)
        {
          return timeMillis - MILLIS_IN;
        }

        @Override
        public long truncate(long timeMillis)
        {
          return QueryGranularity.DAY.truncate(timeMillis);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfDay(0);

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Days.daysIn(interval).getDays();
        }
      },
  WEEK
      {
        DateTimeFormatter format = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd");
        private final int MILLIS_IN = 7 * 24 * 60 * 60 * 1000;

        @Override
        public String toPath(DateTime time)
        {
          throw new UnsupportedOperationException("Not Implemented Yet.");
        }

        @Override
        public DateTime increment(DateTime time)
        {
          return time.plus(Weeks.ONE);
        }

        @Override
        public DateTime increment(DateTime time, int count)
        {
          return time.plus(Weeks.weeks(count));
        }

        @Override
        public long increment(long timeMillis)
        {
          return timeMillis - MILLIS_IN;
        }

        @Override
        public DateTime decrement(DateTime time)
        {
          return time.minus(Weeks.ONE);
        }

        @Override
        public DateTime decrement(DateTime time, int count)
        {
          return time.minus(Weeks.weeks(count));
        }

        @Override
        public long decrement(long timeMillis)
        {
          return timeMillis - MILLIS_IN;
        }

        @Override
        public long truncate(long timeMillis)
        {
          return truncate(new DateTime(timeMillis)).getMillis();
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfDay(0);
          mutableDateTime.setDayOfWeek(1);

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Weeks.weeksIn(interval).getWeeks();
        }
      },
  MONTH
      {
        DateTimeFormatter format = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM");

        @Override
        public String toPath(DateTime time)
        {
          return format.print(time);
        }

        @Override
        public DateTime increment(DateTime time)
        {
          return time.plus(Months.ONE);
        }

        @Override
        public DateTime increment(DateTime time, int count)
        {
          return time.plus(Months.months(count));
        }

        @Override
        public long increment(long timeMillis)
        {
          return new DateTime(timeMillis).plus(Months.ONE).getMillis();
        }

        @Override
        public DateTime decrement(DateTime time)
        {
          return time.minus(Months.ONE);
        }

        @Override
        public DateTime decrement(DateTime time, int count)
        {
          return time.minus(Months.months(count));
        }

        @Override
        public long decrement(long timeMillis)
        {
          return new DateTime(timeMillis).minus(Months.ONE).getMillis();
        }

        @Override
        public long truncate(long timeMillis)
        {
          return truncate(new DateTime(timeMillis)).getMillis();
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfDay(0);
          mutableDateTime.setDayOfMonth(1);

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Months.monthsIn(interval).getMonths();
        }
      },
  LATEST
      {
        DateTimeFormatter format = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm");

        @Override
        public String toPath(DateTime time)
        {
          return format.print(time);
        }

        @Override
        public DateTime increment(DateTime time)
        {
          throw new UnsupportedOperationException("LATEST exists for client-side only");
        }

        @Override
        public DateTime increment(DateTime time, int count)
        {
          throw new UnsupportedOperationException("LATEST exists for client-side only");
        }

        @Override
        public long increment(long timeMillis)
        {
          throw new UnsupportedOperationException("LATEST exists for client-side only");
        }

        @Override
        public DateTime decrement(DateTime time)
        {
          throw new UnsupportedOperationException("LATEST exists for client-side only");
        }

        @Override
        public DateTime decrement(DateTime time, int count)
        {
          throw new UnsupportedOperationException("LATEST exists for client-side only");
        }

        @Override
        public long decrement(long timeMillis)
        {
          throw new UnsupportedOperationException("LATEST exists for client-side only");
        }

        @Override
        public long truncate(long timeMillis)
        {
          return 0;
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          return new DateTime(0L);
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          throw new UnsupportedOperationException("LATEST exists for client-side only");
        }
      };

  public abstract String toPath(DateTime time);

  public abstract DateTime increment(DateTime time);

  public abstract DateTime increment(DateTime time, int count);

  public abstract long increment(long timeMillis);

  public abstract DateTime decrement(DateTime time);

  public abstract DateTime decrement(DateTime time, int count);

  public abstract long decrement(long timeMillis);

  public abstract long truncate(long timeMillis);

  public abstract DateTime truncate(DateTime time);

  public abstract int numIn(ReadableInterval interval);

  @JsonCreator
  public static IndexGranularity fromString(String s)
  {
    return IndexGranularity.valueOf(s.toUpperCase());
  }
}
