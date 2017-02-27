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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.Cacheable;
import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.druid.java.util.common.granularity.Granularities.DAY;
import static io.druid.java.util.common.granularity.Granularities.FIFTEEN_MINUTE;
import static io.druid.java.util.common.granularity.Granularities.FIVE_MINUTE;
import static io.druid.java.util.common.granularity.Granularities.HOUR;
import static io.druid.java.util.common.granularity.Granularities.MINUTE;
import static io.druid.java.util.common.granularity.Granularities.MONTH;
import static io.druid.java.util.common.granularity.Granularities.QUARTER;
import static io.druid.java.util.common.granularity.Granularities.SECOND;
import static io.druid.java.util.common.granularity.Granularities.SIX_HOUR;
import static io.druid.java.util.common.granularity.Granularities.TEN_MINUTE;
import static io.druid.java.util.common.granularity.Granularities.THIRTY_MINUTE;
import static io.druid.java.util.common.granularity.Granularities.WEEK;
import static io.druid.java.util.common.granularity.Granularities.YEAR;

public abstract class Granularity implements Cacheable
{
  /**
   * For a select subset of granularites, users can specify them directly as string.
   * These are "predefined granularities".
   * For all others, the users will have to use "Duration" or "Period" type granularities
   */
  static final List<Granularity> PREDEFINED_GRANULARITIES = ImmutableList.of(
      SECOND, MINUTE, FIVE_MINUTE, TEN_MINUTE, FIFTEEN_MINUTE, THIRTY_MINUTE,
      HOUR, SIX_HOUR, DAY, WEEK, MONTH, QUARTER, YEAR);

  /**
   * Default patterns for parsing paths.
   */
  private static final Pattern defaultPathPattern =
      Pattern.compile(
          "^.*[Yy]=(\\d{4})/(?:[Mm]=(\\d{2})/(?:[Dd]=(\\d{2})/(?:[Hh]=(\\d{2})/(?:[Mm]=(\\d{2})/(?:[Ss]=(\\d{2})/)?)?)?)?)?.*$"
      );
  private static final Pattern hivePathPattern =
      Pattern.compile("^.*dt=(\\d{4})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})?)?)?)?)?)?/.*$");

  @JsonCreator
  public static Granularity fromString(String str)
  {
    return GranularityType.valueOf(str.toUpperCase()).defaultGranularity;
  }

  /**
   * simple merge strategy on query granularity that checks if all are equal or else
   * returns null. this can be improved in future but is good enough for most use-cases.
   */
  public static Granularity mergeGranularities(List<Granularity> toMerge)
  {
    if (toMerge == null || toMerge.size() == 0) {
      return null;
    }

    Granularity result = toMerge.get(0);
    for (int i = 1; i < toMerge.size(); i++) {
      if (!Objects.equals(result, toMerge.get(i))) {
        return null;
      }
    }

    return result;
  }

  public static List<Granularity> granularitiesFinerThan(final Granularity gran0)
  {
    final DateTime epoch = new DateTime(0);
    final List<Granularity> retVal = Lists.newArrayList();
    final DateTime origin = (gran0 instanceof PeriodGranularity) ? ((PeriodGranularity) gran0).getOrigin() : null;
    final DateTimeZone tz = (gran0 instanceof PeriodGranularity) ? ((PeriodGranularity) gran0).getTimeZone() : null;
    for (GranularityType gran : GranularityType.values()) {
      /**
       * All and None are excluded b/c when asked to give all granularities finer
       * than "TEN_MINUTE", you want the answer to be "FIVE_MINUTE, MINUTE and SECOND"
       * it doesn't make sense to include ALL or None to be part of this.
       */
      if (gran == GranularityType.ALL || gran == GranularityType.NONE) {
        continue;
      }
      final Granularity segmentGranularity = gran.create(origin, tz);
      if (segmentGranularity.bucket(epoch).toDurationMillis() <= gran0.bucket(epoch).toDurationMillis()) {
        retVal.add(segmentGranularity);
      }
    }
    Collections.sort(
        retVal,
        new Comparator<Granularity>()
        {
          @Override
          public int compare(Granularity g1, Granularity g2)
          {
            return Longs.compare(g2.bucket(epoch).toDurationMillis(), g1.bucket(epoch).toDurationMillis());
          }
        }
    );
    return retVal;
  }

  public abstract DateTimeFormatter getFormatter(Formatter type);

  public abstract DateTime increment(DateTime time);

  public abstract DateTime decrement(DateTime time);

  public abstract DateTime bucketStart(DateTime time);

  public abstract DateTime toDate(String filePath, Formatter formatter);

  public DateTime bucketEnd(DateTime time) {
    return increment(bucketStart(time));
  }

  public DateTime toDateTime(long offset)
  {
    return new DateTime(offset, DateTimeZone.UTC);
  }

  public DateTime toDate(String filePath)
  {
    return toDate(filePath, Formatter.DEFAULT);
  }

  public final String toPath(DateTime time)
  {
    return getFormatter(Formatter.DEFAULT).print(time);
  }

  /**
   * Return a granularity-sized Interval containing a particular DateTime.
   */
  public final Interval bucket(DateTime t)
  {
    DateTime start = bucketStart(t);
    return new Interval(start, increment(start));
  }

  // Used by the toDate implementations.
  final Integer[] getDateValues(String filePath, Formatter formatter)
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

    // The size is "7" b/c this array contains standard
    // datetime field values namely:
    // year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute,
    // and index 0 is unused.
    Integer[] vals = new Integer[7];
    if (matcher.matches()) {
      for (int i = 1; i <= matcher.groupCount(); i++) {
        vals[i] = (matcher.group(i) != null) ? Integer.parseInt(matcher.group(i)) : null;
      }
    }

    return vals;
  }

  public Iterable<Interval> getIterable(final Interval input)
  {
    return new IntervalIterable(input);
  }

  public enum Formatter
  {
    DEFAULT,
    HIVE,
    LOWER_DEFAULT
  }

  /**
   * Only to create a mapping of the granularity and all the supported file patterns
   * namely: default, lowerDefault and hive.
   */
  public enum GranularityType
  {
    SECOND(
        "'dt'=yyyy-MM-dd-HH-mm-ss",
        "'y'=yyyy/'m'=MM/'d'=dd/'h'=HH/'m'=mm/'s'=ss",
        "'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm/'S'=ss",
        6,
        "PT1S"
    ),
    MINUTE(
        "'dt'=yyyy-MM-dd-HH-mm",
        "'y'=yyyy/'m'=MM/'d'=dd/'h'=HH/'m'=mm",
        "'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm",
        5,
        "PT1M"
    ),
    FIVE_MINUTE(MINUTE, "PT5M"),
    TEN_MINUTE(MINUTE, "PT10M"),
    FIFTEEN_MINUTE(MINUTE, "PT15M"),
    THIRTY_MINUTE(MINUTE, "PT30M"),
    HOUR(
        "'dt'=yyyy-MM-dd-HH",
        "'y'=yyyy/'m'=MM/'d'=dd/'h'=HH",
        "'y'=yyyy/'m'=MM/'d'=dd/'H'=HH",
        4,
        "PT1H"
    ),
    SIX_HOUR(HOUR, "PT6H"),
    DAY(
        "'dt'=yyyy-MM-dd",
        "'y'=yyyy/'m'=MM/'d'=dd",
        "'y'=yyyy/'m'=MM/'d'=dd",
        3,
        "P1D"
    ),
    WEEK(DAY, "P1W"),
    MONTH(
        "'dt'=yyyy-MM",
        "'y'=yyyy/'m'=MM",
        "'y'=yyyy/'m'=MM",
        2,
        "P1M"
    ),
    QUARTER(MONTH, "P3M"),
    YEAR(
        "'dt'=yyyy",
        "'y'=yyyy",
        "'y'=yyyy",
        1,
        "P1Y"
    ),
    ALL(new AllGranularity()),
    NONE(new NoneGranularity());

    private final String hiveFormat;
    private final String lowerDefaultFormat;
    private final String defaultFormat;
    private final int dateValuePositions;
    private final Period period;
    private final Granularity defaultGranularity;

    GranularityType(Granularity specialGranularity)
    {
      this.hiveFormat = null;
      this.lowerDefaultFormat = null;
      this.defaultFormat = null;
      this.dateValuePositions = 0;
      this.period = null;
      this.defaultGranularity = specialGranularity;
    }

    GranularityType(
        final String hiveFormat,
        final String lowerDefaultFormat,
        final String defaultFormat,
        final int dateValuePositions,
        final String period
    )
    {
      this.hiveFormat = hiveFormat;
      this.lowerDefaultFormat = lowerDefaultFormat;
      this.defaultFormat = defaultFormat;
      this.dateValuePositions = dateValuePositions;
      this.period = new Period(period);
      this.defaultGranularity = new PeriodGranularity(this.period, null, null);
    }

    GranularityType(GranularityType granularityType, String period)
    {
      this(
          granularityType.getHiveFormat(),
          granularityType.getLowerDefaultFormat(),
          granularityType.getDefaultFormat(),
          granularityType.dateValuePositions,
          period
      );
    }

    Granularity create(DateTime origin, DateTimeZone tz)
    {
      if (period != null && (origin != null || tz != null)) {
        return new PeriodGranularity(period, origin, tz);
      } else {
        // If All or None granularity, or if origin and tz are both null, return the cached granularity
        return defaultGranularity;
      }
    }

    public Granularity getDefaultGranularity()
    {
      return defaultGranularity;
    }

    public static DateTime getDateTime(GranularityType granularityType, Integer[] vals)
    {
      if (granularityType.dateValuePositions == 0) {
        // All or None granularity
        return null;
      }
      for (int i = 1; i <= granularityType.dateValuePositions; i++) {
        if (vals[i] == null) {
          return null;
        }
      }
      return new DateTime(
          vals[1],
          granularityType.dateValuePositions >= 2 ? vals[2] : 1,
          granularityType.dateValuePositions >= 3 ? vals[3] : 1,
          granularityType.dateValuePositions >= 4 ? vals[4] : 0,
          granularityType.dateValuePositions >= 5 ? vals[5] : 0,
          granularityType.dateValuePositions >= 6 ? vals[6] : 0,
          0
      );
    }

    /**
     * Note: This is only an estimate based on the values in period.
     * This will not work for complicated periods that represent say 1 year 1 day
     */
    public static GranularityType fromPeriod(Period period)
    {
      int[] vals = period.getValues();
      int index = -1;
      for (int i = 0; i < vals.length; i++) {
        if (vals[i] != 0) {
          if (index < 0) {
            index = i;
          } else {
            throw new IAE("Granularity is not supported. [%s]", period);
          }
        }
      }

      switch (index) {
        case 0:
          return GranularityType.YEAR;
        case 1:
          if (vals[index] == 4) {
            return GranularityType.QUARTER;
          }
          else if (vals[index] == 1) {
            return GranularityType.MONTH;
          }
          break;
        case 2:
          return GranularityType.WEEK;
        case 3:
          return GranularityType.DAY;
        case 4:
          if (vals[index] == 6) {
            return GranularityType.SIX_HOUR;
          }
          else if (vals[index] == 1) {
            return GranularityType.HOUR;
          }
          break;
        case 5:
          if (vals[index] == 30) {
            return GranularityType.THIRTY_MINUTE;
          }
          else if (vals[index] == 15) {
            return GranularityType.FIFTEEN_MINUTE;
          }
          else if (vals[index] == 10) {
            return GranularityType.TEN_MINUTE;
          }
          else if (vals[index] == 5) {
            return GranularityType.FIVE_MINUTE;
          }
          else if (vals[index] == 1) {
            return GranularityType.MINUTE;
          }
          break;
        case 6:
          return GranularityType.SECOND;
        default:
          break;
      }
      throw new IAE("Granularity is not supported. [%s]", period);
    }

    public String getHiveFormat()
    {
      return hiveFormat;
    }

    public String getLowerDefaultFormat()
    {
      return lowerDefaultFormat;
    }

    public String getDefaultFormat()
    {
      return defaultFormat;
    }
  }

  private class IntervalIterable implements Iterable<Interval>
  {
    private final Interval inputInterval;

    private IntervalIterable(Interval inputInterval)
    {
      this.inputInterval = inputInterval;
    }

    @Override
    public Iterator<Interval> iterator()
    {
      return new IntervalIterator(inputInterval);
    }

  }

  private class IntervalIterator implements Iterator<Interval>
  {
    private final Interval inputInterval;

    private DateTime currStart;
    private DateTime currEnd;

    private IntervalIterator(Interval inputInterval)
    {
      this.inputInterval = inputInterval;

      currStart = bucketStart(inputInterval.getStart());
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
}
