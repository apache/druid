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

package io.druid.query;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * This interface is in a very early stage and should not be considered stable.
 *
 * The purpose of the QueryWatcher is to give overall visibility into queries running
 * or pending at the QueryRunner level. This is currently used to cancel all the
 * parts of a pending query, but may be expanded in the future to offer more direct
 * visibility into query execution and resource usage.
 *
 * QueryRunners executing any computation asynchronously must register their queries
 * with the QueryWatcher.
 *
 */
public interface QueryWatcher
{
  /**
   * QueryRunners must use this method to register any pending queries.
   *
   * The given future may have cancel(true) called at any time, if cancellation of this query has been requested.
   *
   * @param query a query, which may be a subset of a larger query, as long as the underlying queryId is unchanged
   * @param future the future holding the execution status of the query
   */
  public void registerQuery(Query query, ListenableFuture future);
}
