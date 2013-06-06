/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1.serde;

import com.google.common.collect.Maps;
import com.metamx.common.ISE;

import java.util.Map;

/**
 */
public class ComplexMetrics
{
  private static final Map<String, ComplexMetricSerde> complexSerializers = Maps.newHashMap();

  public static ComplexMetricSerde getSerdeForType(String type)
  {
    return complexSerializers.get(type);
  }

  public static void registerSerde(String type, ComplexMetricSerde serde)
  {
    if (complexSerializers.containsKey(type)) {
      throw new ISE("Serializer for type[%s] already exists.", type);
    }
    complexSerializers.put(type, serde);
  }
}
