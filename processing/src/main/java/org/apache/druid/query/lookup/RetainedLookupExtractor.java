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

package org.apache.druid.query.lookup;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.lang.ref.Cleaner;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holds a lookup extractor together with a retained reference to the resources backing it.
 *
 * Callers with an explicit lifecycle should close this extractor when finished. Callers that hand the extractor to
 * APIs without a close hook rely on the Cleaner fallback to release the retained reference when this extractor becomes
 * unreachable.
 */
public class RetainedLookupExtractor extends LookupExtractor implements Closeable
{
  private static final Cleaner CLEANER = Cleaner.create();

  private final LookupExtractor delegate;
  private final RetainedReferenceCleanup retainedReferenceCleanup;
  private final Cleaner.Cleanable cleanable;

  public static RetainedLookupExtractor create(LookupExtractor delegate, Closeable retainedReference)
  {
    return new RetainedLookupExtractor(delegate, retainedReference);
  }

  private RetainedLookupExtractor(LookupExtractor delegate, Closeable retainedReference)
  {
    this.delegate = delegate;
    this.retainedReferenceCleanup = new RetainedReferenceCleanup(retainedReference);
    this.cleanable = CLEANER.register(this, retainedReferenceCleanup);
  }

  @Nullable
  @Override
  public final String apply(@Nullable String key)
  {
    return delegate.apply(key);
  }

  @Override
  protected final List<String> unapply(@Nullable String value)
  {
    return delegate.unapply(value);
  }

  @Override
  public final Map<String, String> applyAll(Iterable<String> keys)
  {
    return delegate.applyAll(keys);
  }

  @Nullable
  @Override
  public final Iterator<String> unapplyAll(Set<String> values)
  {
    return delegate.unapplyAll(values);
  }

  @Override
  public final boolean supportsAsMap()
  {
    return delegate.supportsAsMap();
  }

  @Override
  public final Map<String, String> asMap()
  {
    return delegate.asMap();
  }

  @Override
  public final byte[] getCacheKey()
  {
    return delegate.getCacheKey();
  }

  @Override
  public final boolean isOneToOne()
  {
    return delegate.isOneToOne();
  }

  @Override
  public final long estimateHeapFootprint()
  {
    return delegate.estimateHeapFootprint();
  }

  @Override
  public void close()
  {
    try {
      retainedReferenceCleanup.close();
    }
    finally {
      cleanable.clean();
    }
  }

  private static class RetainedReferenceCleanup implements Runnable
  {
    private final Closeable retainedReference;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private RetainedReferenceCleanup(Closeable retainedReference)
    {
      this.retainedReference = retainedReference;
    }

    @Override
    public void run()
    {
      closeInternal(false);
    }

    private void close()
    {
      closeInternal(true);
    }

    private void closeInternal(boolean propagateFailure)
    {
      if (closed.compareAndSet(false, true)) {
        try {
          retainedReference.close();
        }
        catch (Exception e) {
          if (propagateFailure) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
