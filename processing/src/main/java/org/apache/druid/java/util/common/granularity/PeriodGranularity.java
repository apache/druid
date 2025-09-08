/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.common.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    Preconditions.checkArgument(!Period.ZERO.equals(period), "zero period is not acceptable in PeriodGranularity!");
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

  @Override
  @JsonProperty("timeZone")
  public DateTimeZone getTimeZone()
  {
    return chronology.getZone();
  }

  @JsonProperty("origin")
  @Nullable
  public DateTime getOrigin()
  {
    return hasOrigin ? DateTimes.utc(origin) : null;
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
  public long bucketStart(long time)
  {
    return truncate(time);
  }

  @Override
  public long increment(long t)
  {
    return chronology.add(period, t, 1);
  }

  @Override
  public DateTime increment(DateTime time)
  {
    return new DateTime(increment(time.getMillis()), getTimeZone());
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
  public boolean isAligned(Interval interval)
  {
    return bucket(interval.getStart()).equals(interval);
  }

  @Override
  public byte[] getCacheKey()
  {
    return StringUtils.toUtf8(getPeriod() + ":" + getTimeZone() + ":" + getOrigin());
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

  /**
   * Returns true if this granularity can be mapped to the target granularity. A granularity can be mapped when each
   * interval of the source fits entirely within a single interval of the target under the given time zone.
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code Period("PT1H")} in UTC can be mapped to {@code Period("P1D")} in UTC,
   *       since every hourly interval is fully contained within some day interval.</li>
   *   <li>{@code Period("PT1H")} in {@code America/Los_Angeles} can be mapped to
   *       {@code Period("PT1H")} in UTC, since each hour in local time still fits inside
   *       a corresponding hour in UTC (even though offsets can differ due to daylight saving).</li>
   *   <li>{@code Period("P1D")} in {@code America/Los_Angeles} cannot be mapped to
   *       {@code Period("P1D")} in UTC, since local day boundaries may cross UTC days and
   *       are not fully contained within a single UTC day.</li>
   *   <li>{@code Period("PT1H")} in {@code Asia/Kolkata} cannot be mapped to
   *       {@code Period("PT1H")} in UTC, since the 30-minute offset causes local hour
   *       intervals to straddle two UTC hour intervals.</li>
   * </ul>
   *
   * @param target the target granularity to check against
   * @return {@code true} if this granularity is fully contained within the target granularity; {@code false} otherwise
   */
  public boolean canBeMappedTo(PeriodGranularity target)
  {
    if (hasOrigin || target.hasOrigin) {
      return false;
    }

    if (getTimeZone().equals(target.getTimeZone())) {
      int periodMonths = period.getYears() * 12 + period.getMonths();
      int targetMonths = target.period.getYears() * 12 + target.period.getMonths();
      if (targetMonths == 0 && periodMonths != 0) {
        // cannot map if target has no month, but period has month, e.x. P1M cannot be mapped to P1D or P1W
        return false;
      }

      Optional<Long> periodStandardSeconds = getStandardSeconds(period.withYears(0).withMonths(0));
      if (periodStandardSeconds.isEmpty()) {
        // millisecond precision period is not supported
        return false;
      }
      Optional<Long> targetStandardSeconds = getStandardSeconds(target.period.withYears(0).withMonths(0));
      if (targetStandardSeconds.isEmpty()) {
        // millisecond precision period is not supported
        return false;
      }
      if (targetMonths == 0 && periodMonths == 0) {
        // both periods have zero months, we only need to check standard seconds
        // e.x. PT1H can be mapped to PT3H, PT15M can be mapped to PT1H
        return targetStandardSeconds.get() % periodStandardSeconds.get() == 0;
      }
      // if we reach here, targetMonths != 0
      if (periodMonths == 0) {
        // can map if 1.target not have week/day/hour/minute/second, and 2.period can be mapped to day
        // e.x PT3H can be mapped to P1M
        return targetStandardSeconds.get() == 0 && (3600 * 24) % periodStandardSeconds.get() == 0;
      } else {
        // can map if 1.target&period not have week/day/hour/minute/second, and 2.period month can be mapped to target month
        // e.x. P1M can be mapped to P3M, P1M can be mapped to P1Y
        return targetMonths % periodMonths == 0
               && targetStandardSeconds.get() == 0
               && periodStandardSeconds.get() == 0;
      }
    }

    // different time zones, we'd map to UTC first, then check if the target can cover the UTC-mapped period
    Optional<Long> standardSeconds = getStandardSeconds(period);
    if (standardSeconds.isEmpty()) {
      // must be in whole seconds, i.e. no years, months, or milliseconds.
      return false;
    }
    Optional<Long> utcMappablePeriodSeconds = getUtcMappablePeriodSeconds();
    if (utcMappablePeriodSeconds.isEmpty()) {
      return false;
    }
    if (!standardSeconds.get().equals(utcMappablePeriodSeconds.get())) {
      // the period cannot be mapped to UTC with the same period, e.x. PT1H in Asia/Kolkata cannot be mapped to PT1H in UTC
      return false;
    }
    if (target.period.getYears() == 0 && target.period.getMonths() == 0) {
      Optional<Long> targetUtcMappablePeriodSeconds = target.getUtcMappablePeriodSeconds();
      if (targetUtcMappablePeriodSeconds.isEmpty()) {
        return false;
      }
      // both periods have zero months, we only need to check standard seconds
      // e.x. PT30M in Asia/Kolkata can be mapped to PT1H in America/Los_Angeles
      return targetUtcMappablePeriodSeconds.get() % standardSeconds.get() == 0;
    } else {
      // can map if 1.target not have week/day/hour/minute/second, and 2.period can be mapped to day
      // e.x PT1H in America/Los_Angeles can be mapped to P1M in Asia/Shanghai
      Optional<Long> targetStandardSecondsIgnoringMonth = getStandardSeconds(target.period.withYears(0).withMonths(0));
      return targetStandardSecondsIgnoringMonth.isPresent()
             && targetStandardSecondsIgnoringMonth.get() == 0
             && (3600 * 24) % standardSeconds.get() == 0;
    }
  }

  /**
   * Returns the maximum possible period seconds that this granularity can be mapped to UTC.
   * <p>
   * Returns empty if the period cannot be mapped to whole seconds, i.e. it has years or months, or milliseconds.
   */
  private Optional<Long> getUtcMappablePeriodSeconds()
  {
    Optional<Long> periodSeconds = PeriodGranularity.getStandardSeconds(period);
    if (periodSeconds.isEmpty()) {
      return Optional.empty();
    }

    if (ISOChronology.getInstanceUTC().getZone().equals(getTimeZone())) {
      return periodSeconds;
    }
    ZoneRules rules = ZoneId.of(getTimeZone().getID()).getRules();
    Set<Integer> offsets = Stream.concat(
        Stream.of(rules.getStandardOffset(Instant.now())),
        rules.getTransitions()
             .stream()
             .filter(t -> t.getInstant().isAfter(Instant.EPOCH)) // timezone transitions before epoch are patchy
             .map(ZoneOffsetTransition::getOffsetBefore)
    ).map(ZoneOffset::getTotalSeconds).collect(Collectors.toSet());

    if (offsets.isEmpty()) {
      // no offsets
      return periodSeconds;
    }

    if (offsets.stream().allMatch(o -> o % periodSeconds.get() == 0)) {
      // all offsets are multiples of the period, e.x. PT8H and PT2H in Asia/Shanghai
      return periodSeconds;
    } else if (periodSeconds.get() % 3600 == 0 && offsets.stream().allMatch(o -> o % 3600 == 0)) {
      // fall back to hour if period is a multiple of hour and all offsets are multiples of hour, e.x. PT1H in America/Los_Angeles
      return Optional.of(3600L);
    } else if (periodSeconds.get() % 1800 == 0 && offsets.stream().allMatch(o -> o % 1800 == 0)) {
      // fall back to hour if period is a multiple of 30 minutes and all offsets are multiples of 30 minutes, e.x. PT30M in Asia/Kolkata
      return Optional.of(1800L);
    } else if (periodSeconds.get() % 60 == 0 && offsets.stream().allMatch(o -> o % 60 == 0)) {
      // fall back to minute if period is a multiple of minute and all offsets are multiples of minute
      return Optional.of(60L);
    } else {
      // default to second
      return Optional.of(1L);
    }
  }

  /**
   * Returns the standard whole seconds for the given period.
   * <p>
   * Returns empty if the period cannot be mapped to whole seconds, i.e. one of the following applies:
   * <ul>
   * <li>it has years or months
   * <li>it has milliseconds
   */
  private static Optional<Long> getStandardSeconds(Period period)
  {
    if (period.getYears() == 0 && period.getMonths() == 0) {
      long millis = period.toStandardDuration().getMillis();
      return millis % 1000 == 0
             ? Optional.of(millis / 1000)
             : Optional.empty();
    }
    return Optional.empty();
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
          t = chronology.years().add(origin, y - years);
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
          t = chronology.months().add(origin, m - months);
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
          t = chronology.weeks().add(origin, w - weeks);
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
          t = chronology.days().add(origin, d - days);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.dayOfMonth().roundFloor(t);
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
        if (t < tt && origin > 0) {
          t = chronology.hours().add(tt, -hours);
        } else if (t > tt && origin < 0) {
          t = chronology.minuteOfHour().roundFloor(tt);
          t = chronology.minuteOfHour().set(t, 0);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.hourOfDay().roundFloor(t);
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
        return chronology.minuteOfHour().roundFloor(t);
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
          "Period cannot be converted to milliseconds as some fields mays vary in length with chronology " + chronology
      );
    }
  }

  @Override
  public void serialize(JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException
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
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider,
      TypeSerializer typeSerializer
  ) throws IOException
  {
    serialize(jsonGenerator, serializerProvider);
  }
}
