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

package com.metamx.druid.realtime.plumber;

import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;

public interface Plumber
{
  /**
   * Perform any initial setup. Should be called before using any other methods, and should be paired
   * with a corresponding call to {@link #finishJob}.
   */
  public void startJob();

  public Sink getSink(long timestamp);
  public <T> QueryRunner<T> getQueryRunner(Query<T> query);

  /**
   * Persist any in-memory indexed data to durable storage. This may be only somewhat durable, e.g. the
   * machine's local disk.
   *
   * @param commitRunnable code to run after persisting data
   */
  void persist(Runnable commitRunnable);

  /**
   * Perform any final processing and clean up after ourselves. Should be called after all data has been
   * fed into sinks and persisted.
   */
  public void finishJob();
}
