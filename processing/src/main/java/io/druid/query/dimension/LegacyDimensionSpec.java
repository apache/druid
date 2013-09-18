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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.metamx.common.IAE;

import java.util.Map;

/**
 */
public class LegacyDimensionSpec extends DefaultDimensionSpec
{
  private static final String convertValue(Object dimension, String name)
  {
    final String retVal;

    if (dimension instanceof String) {
      retVal = (String) dimension;
    } else if (dimension instanceof Map) {
      retVal = (String) ((Map) dimension).get(name);
    } else {
      throw new IAE("Unknown type[%s] for dimension[%s]", dimension.getClass(), dimension);
    }

    return retVal;
  }

  @JsonCreator
  public LegacyDimensionSpec(Object dimension)
  {
    super(convertValue(dimension, "dimension"), convertValue(dimension, "outputName"));
  }
}
