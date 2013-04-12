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

package com.metamx.druid.input;

import com.google.common.collect.Maps;
import com.metamx.common.ISE;

import java.util.List;
import java.util.TreeMap;

/**
 */
public class Rows
{
  public static InputRow toCaseInsensitiveInputRow(final Row row, final List<String> dimensions)
  {
    if (row instanceof MapBasedRow) {
      MapBasedRow mapBasedRow = (MapBasedRow) row;

      TreeMap<String, Object> caseInsensitiveMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
      caseInsensitiveMap.putAll(mapBasedRow.getEvent());
      return new MapBasedInputRow(
          mapBasedRow.getTimestampFromEpoch(),
          dimensions,
          caseInsensitiveMap
      );
    }
    throw new ISE("Can only convert MapBasedRow objects because we are ghetto like that.");
  }
}
