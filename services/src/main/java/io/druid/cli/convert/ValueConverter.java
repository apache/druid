/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class ValueConverter implements PropertyConverter
{
  private final Map<String, String> valueMap;
  private final String property;
  public ValueConverter(String property, Map<String, String> valueMap){
    this.property = property;
    this.valueMap = valueMap;
  }

  @Override
  public boolean canHandle(String property)
  {
    return this.property.equals(property);
  }

  @Override
  public Map<String, String> convert(Properties properties)
  {
    final String oldValue = properties.getProperty(this.property);
    if(null == oldValue){
      return ImmutableMap.of();
    }
    final String newValue = valueMap.get(oldValue);
    if(null == newValue){
      return ImmutableMap.of();
    }
    return ImmutableMap.of(this.property, newValue);
  }
}
