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
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;

import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Granularity
{

  public static final Granularity SECOND = Granularity.fromString("SECOND");
  public static final Granularity MINUTE = Granularity.fromString("MINUTE");
  public static final Granularity FIVE_MINUTE = Granularity.fromString("FIVE_MINUTE");
  public static final Granularity TEN_MINUTE = Granularity.fromString("TEN_MINUTE");
  public static final Granularity FIFTEEN_MINUTE = Granularity.fromString("FIFTEEN_MINUTE");
  public static final Granularity THIRTY_MINUTE = Granularity.fromString("THIRTY_MINUTE");
  public static final Granularity HOUR = Granularity.fromString("HOUR");
  public static final Granularity SIX_HOUR = Granularity.fromString("SIX_HOUR");
  public static final Granularity DAY = Granularity.fromString("DAY");
  public static final Granularity WEEK = Granularity.fromString("WEEK");
  public static final Granularity MONTH = Granularity.fromString("MONTH");
  public static final Granularity QUARTER = Granularity.fromString("QUARTER");
  public static final Granularity YEAR = Granularity.fromString("YEAR");
  public static final Granularity ALL = Granularity.fromString("ALL");
  public static final Granularity NONE = Granularity.fromString("NONE");
  // Default patterns for parsing paths.
  final Pattern defaultPathPattern =
      Pattern.compile(
          "^.*[Yy]=(\\d{4})/(?:[Mm]=(\\d{2})/(?:[Dd]=(\\d{2})/(?:[Hh]=(\\d{2})/(?:[Mm]=(\\d{2})/(?:[Ss]=(\\d{2})/)?)?)?)?)?.*$"
      );
  final Pattern hivePathPattern =
      Pattern.compile("^.*dt=(\\d{4})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})?)?)?)?)?)?/.*$");

  @JsonCreator
  public static Granularity fromString(String str)
  {
    String name = str.toUpperCase();
    return GranularityType.createGranularity(name);
  }

  //simple merge strategy on query granularity that checks if all are equal or else
  //returns null. this can be improved in future but is good enough for most use-cases.
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
      if (gran == GranularityType.ALL || gran == GranularityType.NONE) {
        continue;
      }
      final Granularity segmentGranularity = GranularityType.createGranularity(gran.name(), origin, tz);
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

  public abstract DateTime truncate(DateTime time);

  public abstract DateTime toDate(String filePath, Formatter formatter);

  public abstract byte[] cacheKey();

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
    return toPath(time, "default");
  }

  private final String toPath(DateTime time, String type)
  {
    return toPath(time, Formatter.valueOf(type.toUpperCase()));
  }

  private final String toPath(DateTime time, Formatter type)
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

  public Iterable<Long> iterable(final long start, final long end)
  {
    return new Iterable<Long>()
    {
      @Override
      public Iterator<Long> iterator()
      {
        return new Iterator<Long>()
        {
          long curr = truncate(new DateTime(start)).getMillis();
          long next = increment(new DateTime(curr)).getMillis();

          @Override
          public boolean hasNext()
          {
            return curr < end;
          }

          @Override
          public Long next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            long retVal = curr;

            curr = next;
            next = increment(new DateTime(curr)).getMillis();

            return retVal;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  public enum Formatter
  {
    DEFAULT,
    HIVE,
    LOWER_DEFAULT
  }

  // Only to create a mapping of the granularity and all the supported file patterns
  // namely: default, lowerDefault and hive.
  protected enum GranularityType
  {
    SECOND(
        "'dt'=yyyy-MM-dd-HH-mm-ss",
        "'y'=yyyy/'m'=MM/'d'=dd/'h'=HH/'m'=mm/'s'=ss",
        "'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm/'S'=ss"
    ),
    MINUTE(
        "'dt'=yyyy-MM-dd-HH-mm",
        "'y'=yyyy/'m'=MM/'d'=dd/'h'=HH/'m'=mm",
        "'y'=yyyy/'m'=MM/'d'=dd/'H'=HH/'M'=mm"
    ),
    FIVE_MINUTE(MINUTE),
    TEN_MINUTE(MINUTE),
    FIFTEEN_MINUTE(MINUTE),
    THIRTY_MINUTE(MINUTE),
    HOUR(
        "'dt'=yyyy-MM-dd-HH",
        "'y'=yyyy/'m'=MM/'d'=dd/'h'=HH",
        "'y'=yyyy/'m'=MM/'d'=dd/'H'=HH"
    ),
    SIX_HOUR(HOUR),
    DAY(
        "'dt'=yyyy-MM-dd",
        "'y'=yyyy/'m'=MM/'d'=dd",
        "'y'=yyyy/'m'=MM/'d'=dd"
    ),
    WEEK(DAY),
    MONTH(
        "'dt'=yyyy-MM",
        "'y'=yyyy/'m'=MM",
        "'y'=yyyy/'m'=MM"
    ),
    QUARTER(MONTH),
    YEAR(
        "'dt'=yyyy",
        "'y'=yyyy",
        "'y'=yyyy"
    ),
    ALL(),
    NONE();

    private final String hiveFormat;
    private final String lowerDefaultFormat;
    private final String defaultFormat;

    GranularityType()
    {
      this(null, null, null);
    }

    GranularityType(
        final String hiveFormat,
        final String lowerDefaultFormat, final String defaultFormat
    )
    {
      this.hiveFormat = hiveFormat;
      this.lowerDefaultFormat = lowerDefaultFormat;
      this.defaultFormat = defaultFormat;
    }

    GranularityType(GranularityType granularityType)
    {
      this(
          granularityType.getHiveFormat(),
          granularityType.getLowerDefaultFormat(),
          granularityType.getDefaultFormat()
      );
    }

    static Granularity createGranularity(String str)
    {
      return createGranularity(str, null, null);
    }

    static Granularity createGranularity(String str, DateTime origin, DateTimeZone tz)
    {
      GranularityType granularityType = GranularityType.valueOf(str);

      switch (granularityType) {
        case SECOND:
          return new PeriodGranularity(new Period("PT1S"), origin, tz);
        case MINUTE:
          return new PeriodGranularity(new Period("PT1M"), origin, tz);
        case FIVE_MINUTE:
          return new PeriodGranularity(new Period("PT5M"), origin, tz);
        case TEN_MINUTE:
          return new PeriodGranularity(new Period("PT10M"), origin, tz);
        case FIFTEEN_MINUTE:
          return new PeriodGranularity(new Period("PT15M"), origin, tz);
        case THIRTY_MINUTE:
          return new PeriodGranularity(new Period("PT30M"), origin, tz);
        case HOUR:
          return new PeriodGranularity(new Period("PT1H"), origin, tz);
        case SIX_HOUR:
          return new PeriodGranularity(new Period("PT6H"), origin, tz);
        case DAY:
          return new PeriodGranularity(new Period("P1D"), origin, tz);
        case WEEK:
          return new PeriodGranularity(new Period("P1W"), origin, tz);
        case MONTH:
          return new PeriodGranularity(new Period("P1M"), origin, tz);
        case QUARTER:
          return new PeriodGranularity(new Period("P3M"), origin, tz);
        case YEAR:
          return new PeriodGranularity(new Period("P1Y"), origin, tz);
        case ALL:
          return new AllGranularity();
        case NONE:
          return new NoneGranularity();
        default:
          throw new IAE("[%s] granularity not supported with strings. Try with Period instead", str);
      }
    }

    // Note: This is only an estimate based on the values in period.
    // This will not work for complicated periods that represent say 1 year 1 day
    static GranularityType estimatedGranularityType(Period period)
    {
      int[] vals = period.getValues();
      BitSet bs = new BitSet();
      for (int i = 0; i < vals.length; i++) {
        if (vals[i] != 0) {
          bs.set(i);
        }
      }

      if (bs.cardinality() == 0 || bs.cardinality() > 1) {
        throw new IAE("Granularity is not supported. [%s]", period);
      } else {
        final int index = bs.nextSetBit(0);

        if (index == 0) {
          return GranularityType.YEAR;
        }
        if (index == 1) {
          return GranularityType.MONTH;
        }
        if (index == 2) {
          return GranularityType.WEEK;
        }
        if (index == 3) {
          return GranularityType.DAY;
        }
        if (index == 4) {
          return GranularityType.HOUR;
        }
        if (index == 5) {
          return GranularityType.MINUTE;
        }
        if (index == 6) {
          return GranularityType.SECOND;
        }
      }

      throw new IAE("Granularity is not supported. [%s]", period);
    }

    static DateTime getDateTime(GranularityType gran, Integer[] vals)
    {
      switch (gran) {
        case SECOND: {
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
        case MINUTE:
        case FIVE_MINUTE:
        case TEN_MINUTE:
        case FIFTEEN_MINUTE:
        case THIRTY_MINUTE: {
          DateTime date = null;
          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null && vals[5] != null) {
            date = new DateTime(vals[1], vals[2], vals[3], vals[4], vals[5], 0, 0);
          }
          return date;
        }
        case HOUR:
        case SIX_HOUR: {
          DateTime date = null;
          if (vals[1] != null && vals[2] != null && vals[3] != null && vals[4] != null) {
            date = new DateTime(vals[1], vals[2], vals[3], vals[4], 0, 0, 0);
          }

          return date;
        }
        case DAY:
        case WEEK: {
          DateTime date = null;
          if (vals[1] != null && vals[2] != null && vals[3] != null) {
            date = new DateTime(vals[1], vals[2], vals[3], 0, 0, 0, 0);
          }

          return date;
        }
        case MONTH:
        case QUARTER: {
          DateTime date = null;
          if (vals[1] != null && vals[2] != null) {
            date = new DateTime(vals[1], vals[2], 1, 0, 0, 0, 0);
          }

          return date;
        }
        case YEAR: {
          DateTime date = null;
          if (vals[1] != null) {
            date = new DateTime(vals[1], 1, 1, 0, 0, 0, 0);
          }

          return date;
        }
      }

      return null;
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
}
