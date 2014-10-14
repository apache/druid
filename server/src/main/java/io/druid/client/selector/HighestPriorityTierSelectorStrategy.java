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

package io.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.primitives.Ints;

import java.util.Comparator;

/**
 */
public class HighestPriorityTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private static final Comparator<Integer> comparator = new Comparator<Integer>()
  {
    @Override
    public int compare(Integer o1, Integer o2)
    {
      return Ints.compare(o2, o1);
    }
  };

  @JsonCreator
  public HighestPriorityTierSelectorStrategy(@JacksonInject ServerSelectorStrategy serverSelectorStrategy)
  {
    super(serverSelectorStrategy);
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    return comparator;
  }
}
