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

package com.metamx.druid.merger.coordinator.config;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class RetryPolicyConfig
{
  @Config("druid.indexer.retry.minWaitMillis")
  @Default("10000")
  public abstract long getRetryMinMillis();

  @Config("druid.indexer.retry.maxWaitMillis")
  @Default("60000")
  public abstract long getRetryMaxMillis();

  @Config("druid.indexer.retry.maxRetryCount")
  @Default("10")
  public abstract long getMaxRetryCount();
}
