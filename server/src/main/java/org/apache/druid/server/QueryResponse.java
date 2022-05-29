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

package org.apache.druid.server;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentManager;

public abstract class QueryResponse<T>
{
  public static class FragmentResponse<T> extends QueryResponse<T>
  {
    /**
     * Fragment context is included because the SQL layer adds another
     * operator on top of the sequence returned here, and that operator
     * (if enabled), needs visibility to the fragment context.
     */
    private final FragmentManager fragment;

    public FragmentResponse(
        final FragmentManager fragment,
        final ResponseContext responseContext
    )
    {
      super(responseContext);
      this.fragment = fragment;
    }

    @Override
    public boolean isFragment()
    {
      return true;
    }

    @Override
    public Sequence<T> getResults()
    {
      return fragment.runAsSequence();
    }

    @Override
    public FragmentManager fragment()
    {
      return fragment;
    }
  }

  public static class SequenceResponse<T> extends QueryResponse<T>
  {
    private final Sequence<T> results;

    public SequenceResponse(
        final Sequence<T> results,
        final ResponseContext responseContext
    )
    {
      super(responseContext);
      this.results = results == null ? Sequences.empty() : results;
    }

    @Override
    public boolean isFragment()
    {
      return false;
    }

    @Override
    public Sequence<T> getResults()
    {
      return results;
    }
  }

  private final ResponseContext responseContext;

  public QueryResponse(final ResponseContext responseContext)
  {
    this.responseContext = responseContext;
  }

  public static <T> QueryResponse<T> withEmptyContext(Sequence<T> results)
  {
    return new SequenceResponse<T>(results, ResponseContext.createEmpty());
  }

  public abstract boolean isFragment();

  public abstract Sequence<T> getResults();

  public FragmentManager fragment()
  {
    return null;
  }

  public ResponseContext getResponseContext()
  {
    return responseContext;
  }

  public <U> QueryResponse<U> withSequence(final Sequence<U> results)
  {
    return new SequenceResponse<U>(results, getResponseContext());
  }

  public <U> QueryResponse<U> withRoot()
  {
    return new FragmentResponse<U>(fragment(), getResponseContext());
  }

  public static <T> QueryResponse<T> empty()
  {
    return new SequenceResponse<T>(Sequences.empty(), ResponseContext.createEmpty());
  }
}
