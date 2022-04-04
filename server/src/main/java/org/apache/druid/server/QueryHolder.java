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

import com.google.common.base.Preconditions;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;

/**
 * Holder of a native Druid query.
 *
 * The native Druid query object has query context parameters in it (see {@link Query#getContext()}).
 * During query processing, Druid can add extra parameters as it needs. However, when authorizing context params,
 * only the params that the user sets should be authorized. To separate user params from others,
 * the Druid native query entry uses {@link QueryContext}. After user context params are authorized
 * in {@link QueryLifecycle#authorize}, QueryLifecycle sets the query context back to this query holder
 * using {@link #withContext(QueryContext)}. When callers use query context, they should check first
 * if the query holder has a valid query context using {@link #isValidContext()}.
 */
public class QueryHolder<T>
{
  private final Query<T> delegate;
  private final boolean validContext;

  public QueryHolder(Query<T> delegate)
  {
    this(delegate, false);
  }

  private QueryHolder(Query<T> delegate, boolean validContext)
  {
    this.delegate = delegate;
    this.validContext = validContext;
  }

  public boolean isValidContext()
  {
    return validContext;
  }

  /**
   * This interface leaks the delegate to the outside of the holder. This is not a good pattern
   * since the caller might use query context in the delegate unexpectedly, and thus the usage of
   * this method should be minimized. Currently, this method is only used in {@link QueryLifecycle}
   * which knows when it can use the delegate directly.
   */
  Query<T> getDelegate()
  {
    return delegate;
  }

  public Query<T> getDelegateWithValidContext()
  {
    Preconditions.checkState(validContext, "invalid context");
    return delegate;
  }

  public DataSource getDataSource()
  {
    return delegate.getDataSource();
  }

  public String getType()
  {
    return delegate.getType();
  }

  public QueryHolder<T> withContext(QueryContext context)
  {
    return new QueryHolder<>(delegate.withContext(context.getMergedParams()), true);
  }
}
