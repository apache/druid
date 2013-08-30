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

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.initialization.LogLevelAdjuster;

/**
 */
public abstract class ServerRunnable implements Runnable
{
  private final Logger log;

  public ServerRunnable(Logger log)
  {
    this.log = log;
  }

  protected abstract Injector getInjector();

  @Override
  public void run()
  {
    try {
      LogLevelAdjuster.register();

      final Lifecycle lifecycle = getInjector().getInstance(Lifecycle.class);

      try {
        lifecycle.start();
      }
      catch (Throwable t) {
        log.error(t, "Error when starting up.  Failing.");
        System.exit(1);
      }

      lifecycle.join();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
