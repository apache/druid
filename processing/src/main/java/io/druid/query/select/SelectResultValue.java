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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class SelectResultValue implements Iterable<EventHolder>
{
  private final List<EventHolder> value;

  @JsonCreator
  public SelectResultValue(
      List<?> value
  )
  {
    this.value = (value == null) ? Lists.<EventHolder>newArrayList() : Lists.transform(
        value,
        new Function<Object, EventHolder>()
        {
          @Override
          public EventHolder apply(Object input)
          {
            if (input instanceof EventHolder) {
              return (EventHolder) input;
            } else if (input instanceof Map) {
              long timestamp = (long) ((Map) input).remove("timestamp");
              EventHolder retVal = new EventHolder(timestamp);
              retVal.putAll((Map) input);
              return retVal;
            } else {
              throw new ISE("Unknown type : %s", input.getClass());
            }
          }
        }
    );
  }

  @JsonValue
  public List<EventHolder> getBaseObject()
  {
    return value;
  }

  @Override
  public Iterator<EventHolder> iterator()
  {
    return value.iterator();
  }
}
