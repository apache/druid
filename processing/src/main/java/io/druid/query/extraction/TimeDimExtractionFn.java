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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.ibm.icu.text.SimpleDateFormat;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;

/**
 */
public class TimeDimExtractionFn implements DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x0;

  private final String timeFormat;
  private final SimpleDateFormat timeFormatter;
  private final String resultFormat;
  private final SimpleDateFormat resultFormatter;

  @JsonCreator
  public TimeDimExtractionFn(
      @JsonProperty("timeFormat") String timeFormat,
      @JsonProperty("resultFormat") String resultFormat
  )
  {
    this.timeFormat = timeFormat;
    this.timeFormatter = new SimpleDateFormat(timeFormat);
    this.timeFormatter.setLenient(true);

    this.resultFormat = resultFormat;
    this.resultFormatter = new SimpleDateFormat(resultFormat);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] timeFormatBytes = timeFormat.getBytes(Charsets.UTF_8);
    return ByteBuffer.allocate(1 + timeFormatBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(timeFormatBytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    Date date;
    try {
      date = timeFormatter.parse(dimValue);
    }
    catch (ParseException e) {
      return dimValue;
    }
    return resultFormatter.format(date);
  }

  @JsonProperty("timeFormat")
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @JsonProperty("resultFormat")
  public String getResultFormat()
  {
    return resultFormat;
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "TimeDimExtractionFn{" +
           "timeFormat='" + timeFormat + '\'' +
           ", resultFormat='" + resultFormat + '\'' +
           '}';
  }
}
