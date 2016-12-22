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
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RE;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

public class PeriodSegmentGranularity extends SegmentGranularity implements JsonSerializable
{
  private final GranularityType granularityType;
  private final PeriodGranularity periodGranularity;

  @JsonCreator
  public PeriodSegmentGranularity(
      @JsonProperty("period") Period period,
      @JsonProperty("origin") DateTime origin,
      @JsonProperty("timeZone") DateTimeZone tz
  )
  {
    this.periodGranularity = new PeriodGranularity(period, origin, tz);
    this.granularityType = GranularityType.estimatedGranularityType(period);
  }

  @JsonProperty("period")
  public Period getPeriod()
  {
    return periodGranularity.getPeriod();
  }

  @JsonProperty("timeZone")
  public DateTimeZone getTimeZone()
  {
    return periodGranularity.getTimeZone();
  }

  @JsonProperty("origin")
  public DateTime getOrigin()
  {
    return periodGranularity.getOrigin();
  }

  @Override
  public DateTimeFormatter getFormatter(Formatter type)
  {
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
    return new DateTime(periodGranularity.increment(time.getMillis()));
  }

  @Override
  public DateTime decrement(DateTime time)
  {
    return new DateTime(periodGranularity.decrement(time.getMillis()));
  }

  @Override
  public DateTime truncate(DateTime time)
  {
    return new DateTime(periodGranularity.truncate(time.getMillis()));
  }

  @Override
  public DateTime toDate(String filePath, Formatter formatter)
  {
    Integer[] vals = getDateValues(filePath, formatter);

    DateTime date = GranularityType.getDateTime(granularityType, vals);

    if (date != null) {
      return truncate(date);
    }

    return null;
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

    PeriodSegmentGranularity that = (PeriodSegmentGranularity) o;

    if (granularityType != that.granularityType) {
      return false;
    }
    return periodGranularity.equals(that.periodGranularity);

  }

  @Override
  public int hashCode()
  {
    int result = granularityType.hashCode();
    result = 31 * result + periodGranularity.hashCode();
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

  @Override
  public void serialize(JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException, JsonProcessingException
  {
    // Retain the same behavior as pre-refactor granularity code.
    // i.e. when Granularity class was an enum.
    if (equalsPredefinedGranularities()) {
      jsonGenerator.writeString(granularityType.toString());
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
  ) throws IOException, JsonProcessingException
  {
    serialize(jsonGenerator, serializerProvider);
  }

  private boolean equalsPredefinedGranularities() {
    final GranularityType[] values = GranularityType.values();
    boolean isEqual = false;
    int i = 0;

    while (!isEqual && i != values.length) {
      GranularityType type = values[i];
      switch (type) {
        case SECOND: {
          isEqual = this.equals(SegmentGranularity.SECOND);
          break;
        }
        case MINUTE: {
          isEqual =  this.equals(SegmentGranularity.MINUTE);
          break;
        }
        case FIVE_MINUTE: {
          isEqual =  this.equals(SegmentGranularity.FIVE_MINUTE);
          break;
        }
        case TEN_MINUTE: {
          isEqual =  this.equals(SegmentGranularity.TEN_MINUTE);
          break;
        }
        case FIFTEEN_MINUTE: {
          isEqual =  this.equals(SegmentGranularity.FIFTEEN_MINUTE);
          break;
        }
        case HOUR: {
          isEqual =  this.equals(SegmentGranularity.HOUR);
          break;
        }
        case SIX_HOUR: {
          isEqual =  this.equals(SegmentGranularity.SIX_HOUR);
          break;
        }
        case DAY: {
          isEqual =  this.equals(SegmentGranularity.DAY);
          break;
        }
        case WEEK: {
          isEqual =  this.equals(SegmentGranularity.WEEK);
          break;
        }
        case MONTH: {
          isEqual =  this.equals(SegmentGranularity.MONTH);
          break;
        }
        case YEAR: {
          isEqual =  this.equals(SegmentGranularity.YEAR);
          break;
        }
        default: throw new RE("Unrecognized type.");
      }
      i++;
    }
    return isEqual;
  }
}
