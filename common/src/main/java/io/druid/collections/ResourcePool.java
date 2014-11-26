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

package io.druid.collections;

import java.io.Closeable;

/**
 * A ResourcePool is a pool of resources expressed as a ResourceHolder that is intended to be present throughout the
 * running of the program. The resources themselves are not intended to be closed, but rather be re-used once the
 * ResourceHolder is no longer needed.
 */
public interface ResourcePool<T>
{
  ResourceHolder<T> take();

  /**
   * A ResourceHolder is simply a Supplier that is also Closeable
   * A ResourceHolder DOES NOT NECESSARILY close the resource it is holding.
   * Typically a particular ResourceHolder implementation is paired with a specific ResourcePool to determine
   * proper closing behavior
   */
  public static interface ResourceHolder<T> extends Closeable
  {
    T get();
  }
}
