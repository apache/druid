/*
 * Druid - a distributed column store.
 * Copyright (C) 2014 Yahoo! Inc.
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

package io.druid.segment;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import io.druid.segment.data.IndexedInts;

import java.util.Iterator;

public class NullDimensionSelector implements DimensionSelector
{

  private static final IndexedInts SINGLETON = new IndexedInts() {
    @Override
    public int size() {
      return 1;
    }

    @Override
    public int get(int index) {
      return 0;
    }

    @Override
    public Iterator<Integer> iterator() {
      return Iterators.singletonIterator(0);
    }
  };

  @Override
  public IndexedInts getRow()
  {
    return SINGLETON;
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public String lookupName(int id)
  {
    return null;
  }

  @Override
  public int lookupId(String name)
  {
    return Strings.isNullOrEmpty(name) ? 0 : -1;
  }
}
