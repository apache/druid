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

package io.druid.cli.convert;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class PrefixRename implements PropertyConverter
{
  private final String prefix;
  private final String outputPrefix;

  private final AtomicBoolean ran = new AtomicBoolean(false);

  public PrefixRename(
      String prefix,
      String outputPrefix
  )
  {
    this.prefix = prefix;
    this.outputPrefix = outputPrefix;
  }

  @Override
  public boolean canHandle(String property)
  {
    return property.startsWith(prefix) && !ran.get();
  }

  @Override
  public Map<String, String> convert(Properties properties)
  {
    if (!ran.getAndSet(true)) {
      Map<String, String> retVal = Maps.newLinkedHashMap();

      for (String property : properties.stringPropertyNames()) {
        if (property.startsWith(prefix)) {
          retVal.put(property.replace(prefix, outputPrefix), properties.getProperty(property));
        }
      }

      return retVal;
    }
    return ImmutableMap.of();
  }
}
