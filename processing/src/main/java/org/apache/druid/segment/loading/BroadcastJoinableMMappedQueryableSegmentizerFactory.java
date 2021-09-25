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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.join.table.BroadcastSegmentIndexedTable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class BroadcastJoinableMMappedQueryableSegmentizerFactory implements SegmentizerFactory
{
  private final IndexIO indexIO;
  private final Set<String> keyColumns;

  @JsonCreator
  public BroadcastJoinableMMappedQueryableSegmentizerFactory(
      @JacksonInject IndexIO indexIO,
      @JsonProperty("keyColumns") Set<String> keyColumns
  )
  {
    this.indexIO = indexIO;
    this.keyColumns = keyColumns;
  }

  @JsonProperty
  public Set<String> getKeyColumns()
  {
    return keyColumns;
  }

  @Override
  public Segment factorize(DataSegment dataSegment, File parentDir, boolean lazy, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException
  {
    try {
      return new QueryableIndexSegment(indexIO.loadIndex(parentDir, lazy, loadFailed), dataSegment.getId()) {
        @Nullable
        @Override
        public <T> T as(Class<T> clazz)
        {
          if (clazz.equals(IndexedTable.class)) {
            return (T) new BroadcastSegmentIndexedTable(this, keyColumns, dataSegment.getVersion());
          }
          return super.as(clazz);
        }
      };
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "%s", e.getMessage());
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BroadcastJoinableMMappedQueryableSegmentizerFactory that = (BroadcastJoinableMMappedQueryableSegmentizerFactory) o;
    return Objects.equals(keyColumns, that.keyColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(keyColumns);
  }
}
