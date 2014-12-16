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

package io.druid.query.datasourcemetadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.joda.time.DateTime;

/**
 */
public class DataSourceMetadataResultValue
{
  private final DateTime maxIngestedEventTime;

  @JsonCreator
  public DataSourceMetadataResultValue(
      DateTime maxIngestedEventTime
  )
  {
    this.maxIngestedEventTime = maxIngestedEventTime;
  }

  @JsonValue
  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime;
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

    DataSourceMetadataResultValue that = (DataSourceMetadataResultValue) o;

    if (maxIngestedEventTime != null
        ? !maxIngestedEventTime.equals(that.maxIngestedEventTime)
        : that.maxIngestedEventTime != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return maxIngestedEventTime != null ? maxIngestedEventTime.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "DataSourceMetadataResultValue{" +
           "maxIngestedEventTime=" + maxIngestedEventTime +
           '}';
  }
}
