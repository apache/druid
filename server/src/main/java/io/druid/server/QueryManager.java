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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.query.Query;
import io.druid.query.QueryWatcher;

import java.util.Set;

public class QueryManager implements QueryWatcher
{
  final SetMultimap<String, ListenableFuture> queries;

  public QueryManager()
  {
    this.queries = Multimaps.synchronizedSetMultimap(
        HashMultimap.<String, ListenableFuture>create()
    );
  }

  public boolean cancelQuery(String id) {
    Set<ListenableFuture> futures = queries.removeAll(id);
    boolean success = true;
    for (ListenableFuture future : futures) {
      success = success && future.cancel(true);
    }
    return success;
  }

  public void registerQuery(Query query, final ListenableFuture future)
  {
    final String id = query.getId();
    queries.put(id, future);
    future.addListener(
        new Runnable()
        {
          @Override
          public void run()
          {
            queries.remove(id, future);
          }
        },
        MoreExecutors.sameThreadExecutor()
    );
  }
}
