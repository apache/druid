/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

package io.druid.segment.data;

import com.metamx.common.ISE;

public class BitmapSerde
{
  // default bitmap indices in Druid <= 0.6.x
  public static final Class<ConciseBitmapSerdeFactory> LEGACY_BITMAP_FACTORY = ConciseBitmapSerdeFactory.class;

  // default bitmap indices for Druid >= 0.7.x
  public static final Class<ConciseBitmapSerdeFactory> DEFAULT_BITMAP_FACTORY = ConciseBitmapSerdeFactory.class;

  public static BitmapSerdeFactory createDefaultFactory()
  {
    try {
      return DEFAULT_BITMAP_FACTORY.newInstance();
    } catch(InstantiationException | IllegalAccessException e) {
      // should never happen
      throw new ISE(e, "Unable to instatiate BitmapSerdeFactory");
    }
  }
  public static BitmapSerdeFactory createLegacyFactory()
  {
    try {
      return LEGACY_BITMAP_FACTORY.newInstance();
    } catch(InstantiationException | IllegalAccessException e) {
      // should never happen
      throw new ISE(e, "Unable to instatiate BitmapSerdeFactory");
    }
  }
}
