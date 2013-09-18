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

package io.druid.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 */
public class Execs
{
  public static ExecutorService singleThreaded(String nameFormat)
  {
    return Executors.newSingleThreadExecutor(makeThreadFactory(nameFormat));
  }

  public static ExecutorService multiThreaded(int threads, String nameFormat)
  {
    return Executors.newFixedThreadPool(threads, makeThreadFactory(nameFormat));
  }

  public static ScheduledExecutorService scheduledSingleThreaded(String nameFormat)
  {
    return Executors.newSingleThreadScheduledExecutor(makeThreadFactory(nameFormat));
  }

  public static ThreadFactory makeThreadFactory(String nameFormat)
  {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
  }
}
