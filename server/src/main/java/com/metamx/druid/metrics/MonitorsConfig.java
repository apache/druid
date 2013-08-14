/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.SysMonitor;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 */
public class MonitorsConfig
{
  @JsonProperty("monitors")
  @NotNull
  private List<Class<? extends Monitor>> monitors = ImmutableList.<Class<? extends Monitor>>builder()
                                                                 .add(JvmMonitor.class)
                                                                 .add(SysMonitor.class)
                                                                 .build();

  public List<Class<? extends Monitor>> getMonitors()
  {
    return monitors;
  }

  @Override
  public String toString()
  {
    return "MonitorsConfig{" +
           "monitors=" + monitors +
           '}';
  }
}
