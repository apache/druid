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

package io.druid.query.metadata.metadata;

/**
 */
public class AllColumnIncluderator implements ColumnIncluderator
{
  @Override
  public boolean include(String columnName)
  {
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    return ALL_CACHE_PREFIX;
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof AllColumnIncluderator;
  }

  @Override
  public int hashCode()
  {
    return AllColumnIncluderator.class.hashCode();
  }
}
