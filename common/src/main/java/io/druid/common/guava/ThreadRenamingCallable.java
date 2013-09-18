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

package io.druid.common.guava;

import java.util.concurrent.Callable;

/**
 */
public abstract class ThreadRenamingCallable<T> implements Callable<T>
{
  private final String name;

  public ThreadRenamingCallable(
      String name
  )
  {
    this.name = name;
  }

  @Override
  public final T call()
  {
    final Thread currThread = Thread.currentThread();
    String currName = currThread.getName();
    try {
      currThread.setName(name);
      return doCall();
    }
    finally {
      currThread.setName(currName);
    }
  }

  public abstract T doCall();
}
