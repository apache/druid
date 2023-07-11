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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * The segment reference for the Unnest Data Source.
 * The input column name, output name and the allowSet follow from {@link org.apache.druid.query.UnnestDataSource}
 */
public class UnnestSegmentReference implements SegmentReference
{
  private static final Logger log = new Logger(UnnestSegmentReference.class);

  private final SegmentReference baseSegment;
  private final VirtualColumn unnestColumn;

  @Nullable
  private final DimFilter unnestFilter;



  public UnnestSegmentReference(
      SegmentReference baseSegment,
      VirtualColumn unnestColumn,
      DimFilter unnestFilter
  )
  {
    this.baseSegment = baseSegment;
    this.unnestColumn = unnestColumn;
    this.unnestFilter = unnestFilter;
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    Closer closer = Closer.create();
    try {
      boolean acquireFailed = baseSegment.acquireReferences().map(closeable -> {
        closer.register(closeable);
        return false;
      }).orElse(true);

      if (acquireFailed) {
        CloseableUtils.closeAndWrapExceptions(closer);
        return Optional.empty();
      } else {
        return Optional.of(closer);
      }
    }
    catch (Throwable e) {
      // acquireReferences is not permitted to throw exceptions.
      CloseableUtils.closeAndSuppressExceptions(closer, e::addSuppressed);
      log.warn(e, "Exception encountered while trying to acquire reference");
      return Optional.empty();
    }
  }

  @Override
  public SegmentId getId()
  {
    return baseSegment.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return baseSegment.getDataInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return new UnnestStorageAdapter(
        baseSegment.asStorageAdapter(),
        unnestColumn,
        unnestFilter
    );
  }

  @Override
  public void close() throws IOException
  {
    baseSegment.close();
  }
}
