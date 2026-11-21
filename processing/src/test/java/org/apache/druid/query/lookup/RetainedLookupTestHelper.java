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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.dimension.LookupDimensionSpec;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** lookup fix that detects use after release and keeps handles reachable to exclude cleaner-based cleanup */
public class RetainedLookupTestHelper
{
  private final List<RetainedLookupExtractor> handles = new ArrayList<>();
  private final AtomicInteger releases = new AtomicInteger();
  private final AtomicInteger applications = new AtomicInteger();
  private final LookupExtractorFactoryContainerProvider provider =
      Mockito.mock(LookupExtractorFactoryContainerProvider.class);
  @Nullable
  private RuntimeException applyException;

  public RetainedLookupTestHelper()
  {
    final RetainingLookupExtractorFactory factory = new RetainingLookupExtractorFactory(
        () -> new MapLookupExtractor(ImmutableMap.of("key", "value"), true),
        () -> {
          final RetainedLookupExtractor handle = RetainedLookupExtractor.create(
              new MapLookupExtractor(ImmutableMap.of("key", "value"), true)
              {
                @Nullable
                @Override
                public String apply(@Nullable final String key)
                {
                  Assertions.assertEquals(0, releases.get(), "Lookup used after release");
                  applications.incrementAndGet();
                  if (applyException != null) {
                    throw applyException;
                  }
                  return super.apply(key);
                }
              },
              releases::incrementAndGet
          );
          handles.add(handle);
          return Optional.of(handle);
        }
    );
    Mockito.when(provider.get("lookup"))
           .thenReturn(Optional.of(new LookupExtractorFactoryContainer("v0", factory)));
  }

  public LookupDimensionSpec dimensionSpec(final String dimension, final String outputName)
  {
    return new LookupDimensionSpec(dimension, outputName, null, false, "missing", "lookup", false, provider);
  }

  public void failOnApply(final RuntimeException exception)
  {
    applyException = exception;
  }

  public int getAcquisitions()
  {
    return handles.size();
  }

  public int getReleases()
  {
    return releases.get();
  }

  public int getApplications()
  {
    return applications.get();
  }
}
