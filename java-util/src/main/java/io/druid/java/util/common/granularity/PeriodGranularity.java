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

package io.druid.java.util.common.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

/**
 * PeriodGranularity buckets data based on any custom time period
 */
public class PeriodGranularity extends Granularity implements JsonSerializable
{
  private final Period period;
  private final Chronology chronology;
  private final long origin;
  private final boolean hasOrigin;
  private final boolean isCompound;

  @JsonCreator
  public PeriodGranularity(
      @JsonProperty("period") Period period,
      @JsonProperty("origin") DateTime origin,
      @JsonProperty("timeZone") DateTimeZone tz
  )
  {
    this.period = Preconditions.checkNotNull(period, "period can't be null!");
    Preconditions.checkArgument(!Period.ZERO.equals(period), "zero period is not acceptable in QueryGranularity!");
    this.chronology = tz == null ? ISOChronology.getInstanceUTC() : ISOChronology.getInstance(tz);
    if (origin == null) {
      // default to origin in given time zone when aligning multi-period granularities
      this.origin = new DateTime(0, DateTimeZone.UTC).withZoneRetainFields(chronology.getZone()).getMillis();
      this.hasOrigin = false;
    } else {
      this.origin = origin.getMillis();
      this.hasOrigin = true;
    }
    this.isCompound = isCompoundPeriod(period);
  }

  @JsonProperty("period")
  public Period getPeriod()
  {
    return period;
  }

  @JsonProperty("timeZone")
  public DateTimeZone getTimeZone()
  {
    return chronology.getZone();
  }

  @JsonProperty("origin")
  public DateTime getOrigin()
  {
    return hasOrigin ? new DateTime(origin) : null;
  }

  // Used only for Segments. Not for Queries
  @Override
  public DateTimeFormatter getFormatter(Formatter type)
  {
    GranularityType granularityType = GranularityType.fromPeriod(period);
    switch (type) {
      case DEFAULT:
        return DateTimeFormat.forPattern(granularityType.getDefaultFormat());
      case HIVE:
        return DateTimeFormat.forPattern(granularityType.getHiveFormat());
      case LOWER_DEFAULT:
        return DateTimeFormat.forPattern(granularityType.getLowerDefaultFormat());
      default:
        throw new IAE("There is no format for type %s", type);
    }
  }

  @Override
  public DateTime increment(DateTime time)
  {
    return new DateTime(increment(time.getMillis()), getTimeZone());
  }

  @Override
  public DateTime decrement(DateTime time)
  {
    return new DateTime(decrement(time.getMillis()), getTimeZone());
  }

  @Override
  public DateTime bucketStart(DateTime time)
  {
    return new DateTime(truncate(time.getMillis()), getTimeZone());
  }

  // Used only for Segments. Not for Queries
  @Override
  public DateTime toDate(String filePath, Formatter formatter)
  {
    Integer[] vals = getDateValues(filePath, formatter);
    GranularityType granularityType = GranularityType.fromPeriod(period);

    DateTime date = granularityType.getDateTime(vals);

    if (date != null) {
      return bucketStart(date);
    }

    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    return StringUtils.toUtf8(getPeriod().toString() + ":" +
                              getTimeZone().toString() + ":" + getOrigin());
  }

  @Override
  public DateTime toDateTime(long offset)
  {
    return new DateTime(offset, getTimeZone());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PeriodGranularity that = (PeriodGranularity) o;

    if (origin != that.origin) {
      return false;
    }
    if (hasOrigin != that.hasOrigin) {
      return false;
    }
    if (isCompound != that.isCompound) {
      return false;
    }
    if (!period.equals(that.period)) {
      return false;
    }
    return chronology.equals(that.chronology);

  }

  @Override
  public int hashCode()
  {
    int result = period.hashCode();
    result = 31 * result + chronology.hashCode();
    result = 31 * result + (int) (origin ^ (origin >>> 32));
    result = 31 * result + (hasOrigin ? 1 : 0);
    result = 31 * result + (isCompound ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "{type=period, " +
           "period=" + getPeriod() +
           ", timeZone=" + getTimeZone() +
           ", origin=" + getOrigin() +
           '}';
  }

  private static boolean isCompoundPeriod(Period period)
  {
    int[] values = period.getValues();
    boolean single = false;
    for (int v : values) {
      if (v > 0) {
        if (single) {
          return true;
        }
        single = true;
      }
    }
    return false;
  }

  private long increment(long t)
  {
    return chronology.add(period, t, 1);
  }

  private long decrement(long t)
  {
    return chronology.add(period, t, -1);
  }

  private long truncate(long t)
  {
    if (isCompound) {
      try {
        return truncateMillisPeriod(t);
      }
      catch (UnsupportedOperationException e) {
        return truncateCompoundPeriod(t);
      }
    }

    final int years = period.getYears();
    if (years > 0) {
      if (years > 1 || hasOrigin) {
        int y = chronology.years().getDifference(t, origin);
        y -= y % years;
        long tt = chronology.years().add(origin, y);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.years().add(tt, -years);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.year().roundFloor(t);
      }
    }

    final int months = period.getMonths();
    if (months > 0) {
      if (months > 1 || hasOrigin) {
        int m = chronology.months().getDifference(t, origin);
        m -= m % months;
        long tt = chronology.months().add(origin, m);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.months().add(tt, -months);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.monthOfYear().roundFloor(t);
      }
    }

    final int weeks = period.getWeeks();
    if (weeks > 0) {
      if (weeks > 1 || hasOrigin) {
        // align on multiples from origin
        int w = chronology.weeks().getDifference(t, origin);
        w -= w % weeks;
        long tt = chronology.weeks().add(origin, w);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.weeks().add(tt, -weeks);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.dayOfWeek().roundFloor(t);
        // default to Monday as beginning of the week
        return chronology.dayOfWeek().set(t, 1);
      }
    }

    final int days = period.getDays();
    if (days > 0) {
      if (days > 1 || hasOrigin) {
        // align on multiples from origin
        int d = chronology.days().getDifference(t, origin);
        d -= d % days;
        long tt = chronology.days().add(origin, d);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.days().add(tt, -days);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.hourOfDay().roundFloor(t);
        return chronology.hourOfDay().set(t, 0);
      }
    }

    final int hours = period.getHours();
    if (hours > 0) {
      if (hours > 1 || hasOrigin) {
        // align on multiples from origin
        long h = chronology.hours().getDifferenceAsLong(t, origin);
        h -= h % hours;
        long tt = chronology.hours().add(origin, h);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.hours().add(tt, -hours);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.minuteOfHour().roundFloor(t);
        return chronology.minuteOfHour().set(t, 0);
      }
    }

    final int minutes = period.getMinutes();
    if (minutes > 0) {
      // align on multiples from origin
      if (minutes > 1 || hasOrigin) {
        long m = chronology.minutes().getDifferenceAsLong(t, origin);
        m -= m % minutes;
        long tt = chronology.minutes().add(origin, m);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.minutes().add(tt, -minutes);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.secondOfMinute().roundFloor(t);
        return chronology.secondOfMinute().set(t, 0);
      }
    }

    final int seconds = period.getSeconds();
    if (seconds > 0) {
      // align on multiples from origin
      if (seconds > 1 || hasOrigin) {
        long s = chronology.seconds().getDifferenceAsLong(t, origin);
        s -= s % seconds;
        long tt = chronology.seconds().add(origin, s);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.seconds().add(tt, -seconds);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.millisOfSecond().set(t, 0);
      }
    }

    final int millis = period.getMillis();
    if (millis > 0) {
      if (millis > 1) {
        long ms = chronology.millis().getDifferenceAsLong(t, origin);
        ms -= ms % millis;
        long tt = chronology.millis().add(origin, ms);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.millis().add(tt, -millis);
        } else {
          t = tt;
        }
        return t;
      } else {
        return t;
      }
    }

    return t;
  }

  private long truncateCompoundPeriod(long t)
  {
    long current;
    if (t >= origin) {
      long next = origin;
      do {
        current = next;
        next = chronology.add(period, current, 1);
      } while (t >= next);
    } else {
      current = origin;
      do {
        current = chronology.add(period, current, -1);
      } while (t < current);
    }
    return current;
  }

  private long truncateMillisPeriod(final long t)
  {
    // toStandardDuration assumes days are always 24h, and hours are always 60 minutes,
    // which may not always be the case, e.g if there are daylight saving changes.
    if (chronology.days().isPrecise() && chronology.hours().isPrecise()) {
      final long millis = period.toStandardDuration().getMillis();
      long offset = t % millis - origin % millis;
      if (offset < 0) {
        offset += millis;
      }
      return t - offset;
    } else {
      throw new UnsupportedOperationException(
          "Period cannot be converted to milliseconds as some fields mays vary in length with chronology "
          + chronology.toString()
      );
    }
  }

  @Override
  public void serialize(
      JsonGenerator jsonGenerator, SerializerProvider serializerProvider
  ) throws IOException, JsonProcessingException
  {
    // Retain the same behavior as before #3850.
    // i.e. when Granularity class was an enum.
    if (GranularityType.isStandard(this)) {
      jsonGenerator.writeString(GranularityType.fromPeriod(getPeriod()).toString());
    } else {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("type", "period");
      jsonGenerator.writeObjectField("period", getPeriod());
      jsonGenerator.writeObjectField("timeZone", getTimeZone());
      jsonGenerator.writeObjectField("origin", getOrigin());
      jsonGenerator.writeEndObject();
    }
  }

  @Override
  public void serializeWithType(
      JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer
  ) throws IOException, JsonProcessingException
  {
    serialize(jsonGenerator, serializerProvider);
  }
}
