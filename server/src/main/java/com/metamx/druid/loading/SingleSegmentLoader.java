/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.loading;

import com.google.inject.Inject;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.QueryableIndexSegment;
import com.metamx.druid.index.Segment;

/**
 */
public class SingleSegmentLoader implements SegmentLoader
{
  private final SegmentPuller segmentPuller;
  private final QueryableIndexFactory factory;

  @Inject
  public SingleSegmentLoader(
      SegmentPuller segmentPuller,
      QueryableIndexFactory factory
  )
  {
    this.segmentPuller = segmentPuller;
    this.factory = factory;
  }

  @Override
  public Segment getSegment(DataSegment segment) throws StorageAdapterLoadingException
  {
    final QueryableIndex index = factory.factorize(segmentPuller.getSegmentFiles(segment));

    return new QueryableIndexSegment(segment.getIdentifier(), index);
  }

  @Override
  public void cleanup(DataSegment segment) throws StorageAdapterLoadingException
  {
    segmentPuller.cleanSegmentFiles(segment);
  }
}
