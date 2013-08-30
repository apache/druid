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

package io.druid.segment;

/**
 */
public class Capabilities
{
  private final boolean dimensionValuesSorted;

  public static CapabilitiesBuilder builder()
  {
    return new CapabilitiesBuilder();
  }

  private Capabilities(
      boolean dimensionValuesSorted
  )
  {
    this.dimensionValuesSorted = dimensionValuesSorted;
  }

  public boolean dimensionValuesSorted()
  {
    return dimensionValuesSorted;
  }

  public static class CapabilitiesBuilder
  {
    private boolean dimensionValuesSorted = false;

    private CapabilitiesBuilder() {}

    public CapabilitiesBuilder dimensionValuesSorted(boolean value)
    {
      dimensionValuesSorted = value;
      return this;
    }

    public io.druid.segment.Capabilities build()
    {
      return new io.druid.segment.Capabilities(
          dimensionValuesSorted
      );
    }
  }
}
