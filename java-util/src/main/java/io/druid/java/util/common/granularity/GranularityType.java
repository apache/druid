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

import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

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

  public DateTime getDateTime(Integer[] vals)
  {
    if (dateValuePositions == 0) {
      // All or None granularity
      return null;
    }
    for (int i = 1; i <= dateValuePositions; i++) {
      if (vals[i] == null) {
        return null;
      }
    }
    return new DateTime(
        vals[1],
        dateValuePositions >= 2 ? vals[2] : 1,
        dateValuePositions >= 3 ? vals[3] : 1,
        dateValuePositions >= 4 ? vals[4] : 0,
        dateValuePositions >= 5 ? vals[5] : 0,
        dateValuePositions >= 6 ? vals[6] : 0,
        0
    );
  }

  /**
   * For a select subset of granularites, users can specify them directly as string.
   * These are "predefined granularities" or "standard" granularities.
   * For all others, the users will have to use "Duration" or "Period" type granularities
   */
  public static boolean isStandard(Granularity granularity)
  {
    final GranularityType[] values = GranularityType.values();
    for (GranularityType value : values) {
      if (value.getDefaultGranularity().equals(granularity)) {
        return true;
      }
    }
    return false;
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
        if (vals[index] == 3) {
          return GranularityType.QUARTER;
        } else if (vals[index] == 1) {
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
        } else if (vals[index] == 1) {
          return GranularityType.HOUR;
        }
        break;
      case 5:
        if (vals[index] == 30) {
          return GranularityType.THIRTY_MINUTE;
        } else if (vals[index] == 15) {
          return GranularityType.FIFTEEN_MINUTE;
        } else if (vals[index] == 10) {
          return GranularityType.TEN_MINUTE;
        } else if (vals[index] == 5) {
          return GranularityType.FIVE_MINUTE;
        } else if (vals[index] == 1) {
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
