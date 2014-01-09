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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.metamx.common.IAE;

import java.util.Map;

/**
 */
public class LegacyTopNMetricSpec extends NumericTopNMetricSpec
{
  private static final String convertValue(Object metric)
  {
    final String retVal;

    if (metric instanceof String) {
      retVal = (String) metric;
    } else if (metric instanceof Map) {
      retVal = (String) ((Map) metric).get("metric");
    } else {
      throw new IAE("Unknown type[%s] for metric[%s]", metric.getClass(), metric);
    }

    return retVal;
  }

  @JsonCreator
  public LegacyTopNMetricSpec(Object metric)
  {
    super(convertValue(metric));
  }
}
