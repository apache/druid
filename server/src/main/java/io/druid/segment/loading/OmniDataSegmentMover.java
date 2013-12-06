/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.segment.loading;

import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import io.druid.timeline.DataSegment;

import java.util.Map;

public class OmniDataSegmentMover implements DataSegmentMover
{
  private final Map<String, DataSegmentMover> movers;

  @Inject
  public OmniDataSegmentMover(
      Map<String, DataSegmentMover> movers
  )
  {
    this.movers = movers;
  }

  @Override
  public DataSegment move(DataSegment segment) throws SegmentLoadingException
  {
    return getMover(segment).move(segment);
  }

  private DataSegmentMover getMover(DataSegment segment) throws SegmentLoadingException
  {
    String type = MapUtils.getString(segment.getLoadSpec(), "type");
    DataSegmentMover mover = movers.get(type);

    if (mover == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, movers.keySet());
    }

    return mover;
  }
}
