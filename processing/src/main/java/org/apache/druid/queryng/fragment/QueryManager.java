/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.queryng.fragment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Tracker for fragments that make up a query. For the current two-tier
 * scatter-gather architecture, the root is logical fragment 1, and there
 * is only one instance. The second tier occurs on the broker where there
 * is logical fragment 2 that handles the Broker-side scatter. Then, there
 * is a third tier on data nodes, not represented here, with logical
 * level 3.
 */
public class QueryManager
{
  public static class FragmentTracker
  {
    public final FragmentManager fragment;
    public final int sliceId;
    public final int instanceId;

    public FragmentTracker(FragmentManager fragment, int sliceId, int instanceId)
    {
      this.fragment = fragment;
      this.sliceId = sliceId;
      this.instanceId = instanceId;
    }
  }

  private final String queryId;
  private final Query<?> rootQuery;
  private final Map<FragmentManager, FragmentTracker> fragments = new IdentityHashMap<>();
  private final long startTimeMs;
  private FragmentManager rootFragment;
  private long endTimeMs;

  public QueryManager(String queryId)
  {
    this.rootQuery = null;
    this.queryId = queryId;
    this.startTimeMs = System.currentTimeMillis();
  }

  public QueryManager(Query<?> rootQuery)
  {
    this.rootQuery = rootQuery;
    this.queryId = rootQuery.getId();
    this.startTimeMs = System.currentTimeMillis();
  }

  @VisibleForTesting
  public static QueryManager createWithRoot(
      String queryId,
      final ResponseContext responseContext
  )
  {
    QueryManager queryManager = new QueryManager(queryId);
    queryManager.createRootFragment(responseContext);
    return queryManager;
  }

  public FragmentManager createRootFragment(final ResponseContext responseContext)
  {
    rootFragment = new FragmentManager(
        this,
        queryId,
        responseContext
    );
    FragmentTracker tracker = new FragmentTracker(rootFragment, 1, 1);
    fragments.put(rootFragment, tracker);
    return rootFragment;
  }

  public FragmentManager createChildFragment(
      String queryId,
      ResponseContext responseContext
  )
  {
    FragmentManager fragment = new FragmentManager(
        this,
        queryId,
        responseContext
    );
    FragmentTracker tracker = new FragmentTracker(fragment, 2, fragments.size() + 1);
    fragments.put(fragment, tracker);
    return fragment;
  }

  public Query<?> rootQuery()
  {
    return rootQuery;
  }

  public String queryId()
  {
    return queryId;
  }

  public FragmentManager rootFragment()
  {
    return rootFragment;
  }

  public Map<FragmentManager, FragmentTracker> fragments()
  {
    return fragments;
  }

  public void close()
  {
    if (endTimeMs != 0) {
      return;
    }
    try {
      if (rootFragment != null) {
        rootFragment.close();
      }
    }
    catch (Exception e) {
      rootFragment.failed(e);
    }
    finally {
      endTimeMs = System.currentTimeMillis();
    }
    for (FragmentManager fragment : fragments.keySet()) {
      Preconditions.checkState(fragment.state() == FragmentContext.State.CLOSED);
    }
  }

  public long runTimeMs()
  {
    long endTime = endTimeMs == 0 ? System.currentTimeMillis() : endTimeMs;
    return endTime - startTimeMs;
  }
}
