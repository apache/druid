/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.ReadableInterval;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public enum Granularity
{
  SECOND
      {
        final DateTimeFormatter defaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm/'S'=ss");
        final DateTimeFormatter hiveFormat = DateTimeFormat.forPattern("'dt'=yyyy-MM-dd-HH-mm-ss");
        final DateTimeFormatter lowerDefaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'h'=HH/'m'=mm/'s'=ss");

        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          switch (type) {
            case DEFAULT:
              return defaultFormat;
            case HIVE:
              return hiveFormat;
            case LOWER_DEFAULT:
              return lowerDefaultFormat;
            default:
              throw new IAE("There is no format for type %s at granularity %s", type, this.name());
          }
        }

        @Override
        public ReadablePeriod getUnits(int count)
        {
          return Seconds.seconds(count);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfSecond(0);

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Seconds.secondsIn(interval).getSeconds();
        }

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          DateTime date = null;
          if (vals[1] != null
              && vals[2] != null
              && vals[3] != null
              && vals[4] != null
              && vals[5] != null
              && vals[6] != null) {
            date = new DateTime(vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], 0);
          }

          return date;
        }
      },
  MINUTE
      {
        final DateTimeFormatter defaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm");
        final DateTimeFormatter hiveFormat = DateTimeFormat.forPattern("'dt'=yyyy-MM-dd-HH-mm");
        final DateTimeFormatter lowerDefaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'h'=HH/'m'=mm");

        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          switch (type) {
            case DEFAULT:
              return defaultFormat;
            case HIVE:
              return hiveFormat;
            case LOWER_DEFAULT:
              return lowerDefaultFormat;
            default:
              throw new IAE("There is no format for type %s at granularity %s", type, this.name());
          }
        }

        @Override
        public ReadablePeriod getUnits(int count)
        {
          return Minutes.minutes(count);
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

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          DateTime date = null;
          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null && vals[5] != null) {
            date = new DateTime(vals[1], vals[2], vals[3], vals[4], vals[5], 0, 0);
          }

          return date;
        }
      },
  FIVE_MINUTE
      {
        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          return MINUTE.getFormatter(type);
        }

        @Override
        public ReadablePeriod getUnits(int count)
        {
          return Minutes.minutes(count * 5);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfSecond(0);
          mutableDateTime.setSecondOfMinute(0);
          mutableDateTime.setMinuteOfHour(mutableDateTime.getMinuteOfHour() - (mutableDateTime.getMinuteOfHour() % 5));

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Minutes.minutesIn(interval).getMinutes() / 5;
        }

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null && vals[5] != null) {
            return truncate(new DateTime(vals[1], vals[2], vals[3], vals[4], vals[5], 0, 0));
          }

          return null;
        }
      },
  TEN_MINUTE
      {
        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          return MINUTE.getFormatter(type);
        }

        @Override
        public ReadablePeriod getUnits(int count)
        {
          return Minutes.minutes(count * 10);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfSecond(0);
          mutableDateTime.setSecondOfMinute(0);
          mutableDateTime.setMinuteOfHour(mutableDateTime.getMinuteOfHour() - (mutableDateTime.getMinuteOfHour() % 10));

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Minutes.minutesIn(interval).getMinutes() / 10;
        }

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null && vals[5] != null) {
            return truncate(new DateTime(vals[1], vals[2], vals[3], vals[4], vals[5], 0, 0));
          }

          return null;
        }
      },
  FIFTEEN_MINUTE
      {
        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          return MINUTE.getFormatter(type);
        }

        @Override
        public ReadablePeriod getUnits(int count)
        {
          return Minutes.minutes(count * 15);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfSecond(0);
          mutableDateTime.setSecondOfMinute(0);
          mutableDateTime.setMinuteOfHour(mutableDateTime.getMinuteOfHour() - (mutableDateTime.getMinuteOfHour() % 15));

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Minutes.minutesIn(interval).getMinutes() / 15;
        }

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null && vals[5] != null) {
            return truncate(new DateTime(vals[1], vals[2], vals[3], vals[4], vals[5], 0, 0));
          }

          return null;
        }
      },
  HOUR
      {
        final DateTimeFormatter defaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'H'=HH");
        final DateTimeFormatter hiveFormat = DateTimeFormat.forPattern("'dt'=yyyy-MM-dd-HH");
        final DateTimeFormatter lowerDefaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd/'h'=HH");

        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          switch (type) {
            case DEFAULT:
              return defaultFormat;
            case HIVE:
              return hiveFormat;
            case LOWER_DEFAULT:
              return lowerDefaultFormat;
            default:
              throw new IAE("There is no format for type %s at granularity %s", type, this.name());
          }
        }

        @Override
        public ReadablePeriod getUnits(int n)
        {
          return Hours.hours(n);
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

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          DateTime date = null;
          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null) {
            date = new DateTime(vals[1], vals[2], vals[3], vals[4], 0, 0, 0);
          }

          return date;
        }
      },
  SIX_HOUR
      {
        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          return HOUR.getFormatter(type);
        }

        @Override
        public ReadablePeriod getUnits(int n)
        {
          return Hours.hours(n * 6);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfSecond(0);
          mutableDateTime.setSecondOfMinute(0);
          mutableDateTime.setMinuteOfHour(0);
          mutableDateTime.setHourOfDay(mutableDateTime.getHourOfDay() - (mutableDateTime.getHourOfDay() % 6));

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Hours.hoursIn(interval).getHours() / 6;
        }

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null) {
            return truncate(new DateTime(vals[1], vals[2], vals[3], vals[4], 0, 0, 0));
          }
          return null;
        }
      },
  DAY
      {
        final DateTimeFormatter defaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd");
        final DateTimeFormatter hiveFormat = DateTimeFormat.forPattern("'dt'=yyyy-MM-dd");
        final DateTimeFormatter lowerDefaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM/'d'=dd");

        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          switch (type) {
            case DEFAULT:
              return defaultFormat;
            case HIVE:
              return hiveFormat;
            case LOWER_DEFAULT:
              return lowerDefaultFormat;
            default:
              throw new IAE("There is no format for type %s at granularity %s", type, this.name());
          }
        }

        @Override
        public ReadablePeriod getUnits(int n)
        {
          return Days.days(n);
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

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          DateTime date = null;
          if (vals[1] != null && vals[2] != null && vals[3] != null) {
            date = new DateTime(vals[1], vals[2], vals[3], 0, 0, 0, 0);
          }

          return date;
        }
      },
  WEEK
      {
        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          return DAY.getFormatter(type);
        }

        @Override
        public ReadablePeriod getUnits(int n)
        {
          return Weeks.weeks(n);
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

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          DateTime date = null;
          if (vals[1] != null && vals[2] != null && vals[3] != null) {
            date = truncate(new DateTime(vals[1], vals[2], vals[3], 0, 0, 0, 0));
          }

          return date;
        }
      },
  MONTH
      {
        final DateTimeFormatter defaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM");
        final DateTimeFormatter hiveFormat = DateTimeFormat.forPattern("'dt'=yyyy-MM");
        final DateTimeFormatter lowerDefaultFormat = DateTimeFormat.forPattern("'y'=yyyy/'m'=MM");

        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          switch (type) {
            case DEFAULT:
              return defaultFormat;
            case HIVE:
              return hiveFormat;
            case LOWER_DEFAULT:
              return lowerDefaultFormat;
            default:
              throw new IAE("There is no format for type %s at granularity %s", type, this.name());
          }
        }

        @Override
        public ReadablePeriod getUnits(int n)
        {
          return Months.months(n);
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

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          DateTime date = null;
          if (vals[1] != null && vals[2] != null) {
            date = new DateTime(vals[1], vals[2], 1, 0, 0, 0, 0);
          }

          return date;
        }
      },
  YEAR
      {
        final DateTimeFormatter defaultFormat = DateTimeFormat.forPattern("'y'=yyyy");
        final DateTimeFormatter hiveFormat = DateTimeFormat.forPattern("'dt'=yyyy");
        final DateTimeFormatter lowerDefaultFormat = DateTimeFormat.forPattern("'y'=yyyy");

        @Override
        public DateTimeFormatter getFormatter(Formatter type)
        {
          switch (type) {
            case DEFAULT:
              return defaultFormat;
            case HIVE:
              return hiveFormat;
            case LOWER_DEFAULT:
              return lowerDefaultFormat;
            default:
              throw new IAE("There is no format for type %s at granularity %s", type, this.name());
          }
        }

        @Override
        public ReadablePeriod getUnits(int n)
        {
          return Years.years(n);
        }

        @Override
        public DateTime truncate(DateTime time)
        {
          final MutableDateTime mutableDateTime = time.toMutableDateTime();

          mutableDateTime.setMillisOfDay(0);
          mutableDateTime.setDayOfMonth(1);
          mutableDateTime.setMonthOfYear(1);

          return mutableDateTime.toDateTime();
        }

        @Override
        public int numIn(ReadableInterval interval)
        {
          return Years.yearsIn(interval).getYears();
        }

        @Override
        public DateTime toDate(String filePath, Formatter formatter)
        {
          Integer[] vals = getDateValues(filePath, formatter);

          DateTime date = null;
          if (vals[1] != null) {
            date = new DateTime(vals[1], 1, 1, 0, 0, 0, 0);
          }

          return date;
        }
      };


  // Default patterns for parsing paths.
  protected final Pattern defaultPathPattern =
      Pattern.compile(
          "^.*[Yy]=(\\d{4})/(?:[Mm]=(\\d{2})/(?:[Dd]=(\\d{2})/(?:[Hh]=(\\d{2})/(?:[Mm]=(\\d{2})/(?:[Ss]=(\\d{2})/)?)?)?)?)?.*$"
      );
  protected final Pattern hivePathPattern =
      Pattern.compile("^.*dt=(\\d{4})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})?)?)?)?)?)?/.*$");

  // Abstract functions that individual enum's need to implement for the strategy.
  public abstract DateTimeFormatter getFormatter(Formatter type);

  public abstract ReadablePeriod getUnits(int n);

  public abstract DateTime truncate(DateTime time);

  public abstract int numIn(ReadableInterval interval);

  public abstract DateTime toDate(String filePath, Formatter formatter);

  public DateTime toDate(String filePath)
  {
    return toDate(filePath, Formatter.DEFAULT);
  }

  // Used by the toDate implementations.
  protected final Integer[] getDateValues(String filePath, Formatter formatter)
  {
    Pattern pattern = defaultPathPattern;
    switch (formatter) {
      case DEFAULT:
      case LOWER_DEFAULT:
        break;
      case HIVE:
        pattern = hivePathPattern;
        break;
      default:
        throw new IAE("Format %s not supported", formatter);
    }

    Matcher matcher = pattern.matcher(filePath);

    Integer[] vals = new Integer[7];
    if (matcher.matches()) {
      for (int i = 1; i <= matcher.groupCount(); i++) {
        vals[i] = (matcher.group(i) != null) ? Integer.parseInt(matcher.group(i)) : null;
      }
    }

    return vals;
  }

  // Strategy Functions
  public final DateTimeFormatter getFormatter(String type)
  {
    return getFormatter(Formatter.valueOf(type.toUpperCase()));
  }

  public final DateTime increment(DateTime time)
  {
    return time.plus(getUnits(1));
  }

  public final DateTime increment(DateTime time, int count)
  {
    return time.plus(getUnits(count));
  }

  public final DateTime decrement(DateTime time)
  {
    return time.minus(getUnits(1));
  }

  public final DateTime decrement(DateTime time, int count)
  {
    return time.minus(getUnits(count));
  }

  public final String toPath(DateTime time)
  {
    return toPath(time, "default");
  }

  public final String toPath(DateTime time, String type)
  {
    return toPath(time, Formatter.valueOf(type.toUpperCase()));
  }

  public final String toPath(DateTime time, Formatter type)
  {
    return getFormatter(type).print(time);
  }

  /**
   * Return a granularity-sized Interval containing a particular DateTime.
   */
  public final Interval bucket(DateTime t)
  {
    DateTime start = truncate(t);
    return new Interval(start, increment(start));
  }

  /**
   * Round out Interval such that it becomes granularity-aligned and nonempty.
   */
  public final Interval widen(Interval interval)
  {
    final DateTime start = truncate(interval.getStart());
    final DateTime end;

    if (interval.getEnd().equals(start)) {
      // Empty with aligned start/end; expand into a granularity-sized interval
      end = increment(start);
    } else if (truncate(interval.getEnd()).equals(interval.getEnd())) {
      // Non-empty with aligned end; keep the same end
      end = interval.getEnd();
    } else {
      // Non-empty with non-aligned end; push it out
      end = increment(truncate(interval.getEnd()));
    }

    return new Interval(start, end);
  }

  // Iterable functions and classes.
  public Iterable<Interval> getIterable(final DateTime start, final DateTime end)
  {
    return getIterable(new Interval(start, end));
  }

  public Iterable<Interval> getIterable(final Interval input)
  {
    return new IntervalIterable(input);
  }

  public Iterable<Interval> getReverseIterable(final DateTime start, final DateTime end)
  {
    return getReverseIterable(new Interval(start, end));
  }

  public Iterable<Interval> getReverseIterable(final Interval input)
  {
    return new ReverseIntervalIterable(input);
  }

  public class IntervalIterable implements Iterable<Interval>
  {
    private final Interval inputInterval;

    public IntervalIterable(Interval inputInterval)
    {
      this.inputInterval = inputInterval;
    }

    @Override
    public Iterator<Interval> iterator()
    {
      return new IntervalIterator(inputInterval);
    }

  }

  public class ReverseIntervalIterable implements Iterable<Interval>
  {
    private final Interval inputInterval;

    public ReverseIntervalIterable(Interval inputInterval)
    {
      this.inputInterval = inputInterval;
    }

    @Override
    public Iterator<Interval> iterator()
    {
      return new ReverseIntervalIterator(inputInterval);
    }

  }

  public class IntervalIterator implements Iterator<Interval>
  {
    private final Interval inputInterval;

    private DateTime currStart;
    private DateTime currEnd;

    public IntervalIterator(Interval inputInterval)
    {
      this.inputInterval = inputInterval;

      currStart = truncate(inputInterval.getStart());
      currEnd = increment(currStart);
    }

    @Override
    public boolean hasNext()
    {
      return currStart.isBefore(inputInterval.getEnd());
    }

    @Override
    public Interval next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException("There are no more intervals");
      }
      Interval retVal = new Interval(currStart, currEnd);

      currStart = currEnd;
      currEnd = increment(currStart);

      return retVal;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public class ReverseIntervalIterator implements Iterator<Interval>
  {
    private final Interval inputInterval;

    private DateTime currStart;
    private DateTime currEnd;

    public ReverseIntervalIterator(Interval inputInterval)
    {
      this.inputInterval = inputInterval;

      currEnd = inputInterval.getEnd();
      currStart = decrement(currEnd);

    }

    @Override
    public boolean hasNext()
    {
      return currEnd.isAfter(inputInterval.getStart());
    }

    @Override
    public Interval next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException("There are no more intervals");
      }
      Interval retVal = new Interval(currStart, currEnd);

      currEnd = currStart;
      currStart = decrement(currEnd);

      return retVal;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public enum Formatter
  {
    DEFAULT,
    HIVE,
    LOWER_DEFAULT
  }
}
