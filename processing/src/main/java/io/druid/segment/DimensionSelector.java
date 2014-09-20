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

package io.druid.segment;import io.druid.segment.data.IndexedInts;

/**
 */
public interface DimensionSelector
{
  /**
   * Gets all values for the row inside of an IntBuffer.  I.e. one possible implementation could be
   *
   * return IntBuffer.wrap(lookupExpansion(get());
   *
   * @return all values for the row as an IntBuffer
   */
  public IndexedInts getRow();

  /**
   * Value cardinality is the cardinality of the different occurring values.  If there were 4 rows:
   *
   * A,B
   * A
   * B
   * A
   *
   * Value cardinality would be 2.
   *
   * @return
   */
  public int getValueCardinality();

  /**
   * The Name is the String name of the actual field.  It is assumed that storage layers convert names
   * into id values which can then be used to get the string value.  For example
   *
   * A,B
   * A
   * A,B
   * B
   *
   * getRow() would return
   *
   * getRow(0) => [0 1]
   * getRow(1) => [0]
   * getRow(2) => [0 1]
   * getRow(3) => [1]
   *
   * and then lookupName would return:
   *
   * lookupName(0) => A
   * lookupName(1) => B
   *
   * @param id
   * @return
   */
  public String lookupName(int id);

  /**
   * The ID is the int id value of the field.
   *
   * @param name
   * @return
   */
  public int lookupId(String name);
}
