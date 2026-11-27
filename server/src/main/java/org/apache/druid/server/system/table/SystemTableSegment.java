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

package org.apache.druid.server.system.table;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.segment.ArrayListSegment;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * The rows of a system table on this server, as read by one caller.
 */
public class SystemTableSegment implements Segment
{
  private final SystemTableDataProvider provider;
  private final RowSignature rowSignature;
  private final SystemTableAccess access;
  private final SystemTableCursorFactory cursorFactory;

  public SystemTableSegment(
      final SystemTableDataProvider provider,
      final RowSignature rowSignature,
      final SystemTableAccess access
  )
  {
    this.provider = provider;
    this.rowSignature = rowSignature;
    this.access = access;
    this.cursorFactory = new SystemTableCursorFactory(provider, rowSignature, access);
  }

  @Nullable
  @Override
  public SegmentId getId()
  {
    return null;
  }

  @Override
  public Interval getDataInterval()
  {
    return Intervals.ETERNITY;
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(@Nonnull final Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz)) {
      return (T) cursorFactory;
    } else if (CloseableShapeshifter.class.equals(clazz)) {
      // Window queries read the segment as RowsAndColumns, which needs every row the caller may read.
      final ArrayList<Object[]> rows = new ArrayList<>();
      access.filter(provider.getRows(List.of())).forEach(rows::add);
      return new ArrayListSegment<>(rows, cursorFactory.getRowAdapter(), rowSignature).as(clazz);
    }
    return null;
  }

  @Override
  public void close()
  {
    // Nothing to release.
  }
}
