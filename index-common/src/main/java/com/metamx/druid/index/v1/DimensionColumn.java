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

package com.metamx.druid.index.v1;

/**
 */
public class DimensionColumn
{
  final int[][] dimensionLookups;
  final int[] dimensionValues;

  public DimensionColumn(
      int[][] dimensionLookups,
      int[] dimensionValues
  )
  {
    this.dimensionLookups = dimensionLookups;
    this.dimensionValues = dimensionValues;
  }

  public int[] getDimValues(int rowId)
  {
    return dimensionLookups[dimensionValues[rowId]];
  }

  public int[][] getDimensionExpansions()
  {
    return dimensionLookups;
  }

  public int[] getDimensionRowValues()
  {
    return dimensionValues;
  }
}
