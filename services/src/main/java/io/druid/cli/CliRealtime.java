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

import com.google.common.collect.ImmutableList;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.guice.RealtimeModule;

import java.util.List;

/**
 */
@Command(
    name = "realtime",
    description = "Runs a realtime node, see http://druid.io/docs/0.6.23/Realtime.html for a description"
)
public class CliRealtime extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliRealtime()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new RealtimeModule()
    );
  }
}
