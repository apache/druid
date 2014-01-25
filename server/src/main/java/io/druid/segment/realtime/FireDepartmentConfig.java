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

package io.druid.segment.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Period;

/**
 */
public class FireDepartmentConfig
{
  private final int maxRowsInMemory;
  private final Period intermediatePersistPeriod;

  @JsonCreator
  public FireDepartmentConfig(
      @JsonProperty("maxRowsInMemory") int maxRowsInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod
  )
  {
    this.maxRowsInMemory = maxRowsInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod;

    Preconditions.checkArgument(maxRowsInMemory > 0, "maxRowsInMemory[%s] should be greater than 0", maxRowsInMemory);
    Preconditions.checkNotNull(intermediatePersistPeriod, "intermediatePersistPeriod");
  }

  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }
}
