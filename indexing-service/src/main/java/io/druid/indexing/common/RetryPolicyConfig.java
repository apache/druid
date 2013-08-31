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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

/**
 */
public class RetryPolicyConfig
{
  @JsonProperty
  private Period minWait = new Period("PT1M");

  @JsonProperty
  private Period maxWait = new Period("PT10M");

  @JsonProperty
  private long maxRetryCount = 10;

  public Period getMinWait()
  {
    return minWait;
  }

  RetryPolicyConfig setMinWait(Period minWait)
  {
    this.minWait = minWait;
    return this;
  }

  public Period getMaxWait()
  {
    return maxWait;
  }

  RetryPolicyConfig setMaxWait(Period maxWait)
  {
    this.maxWait = maxWait;
    return this;
  }

  public long getMaxRetryCount()
  {
    return maxRetryCount;
  }

  RetryPolicyConfig setMaxRetryCount(long maxRetryCount)
  {
    this.maxRetryCount = maxRetryCount;
    return this;
  }
}
