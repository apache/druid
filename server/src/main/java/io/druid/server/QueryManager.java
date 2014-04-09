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

package io.druid.server;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.query.Query;
import io.druid.query.QueryWatcher;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

public class QueryManager implements QueryWatcher
{
  final ConcurrentMap<String, ListenableFuture> queries;

  public QueryManager() {
    this.queries = Maps.newConcurrentMap();
  }

  public void cancelQuery(String id) {
    Future future = queries.get(id);
    if(future != null) {
      future.cancel(true);
    }
  }
  public void registerQuery(Query query, ListenableFuture future)
  {
    queries.put(query.getId(), future);
  }
}
