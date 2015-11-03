/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.loading;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;

import java.io.File;
import java.io.IOException;

/**
 */
public class MMappedQueryableIndexFactory implements QueryableIndexFactory
{
  private static final Logger log = new Logger(MMappedQueryableIndexFactory.class);

  private final IndexIO indexIO;

  @Inject
  public MMappedQueryableIndexFactory(IndexIO indexIO)
  {
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
  }

  @Override
  public QueryableIndex factorize(File parentDir) throws SegmentLoadingException
  {
    try {
      return indexIO.loadIndex(parentDir);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "%s", e.getMessage());
    }
  }
}
