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

public class OmniDataSegmentArchiver implements DataSegmentArchiver
{
  private final Map<String, DataSegmentArchiver> archivers;

  @Inject
  public OmniDataSegmentArchiver(
      Map<String, DataSegmentArchiver> archivers
  )
  {
    this.archivers = archivers;
  }

  @Override
  public DataSegment archive(DataSegment segment) throws SegmentLoadingException
  {
    return getArchiver(segment).archive(segment);
  }

  @Override
  public DataSegment restore(DataSegment segment) throws SegmentLoadingException
  {
    return getArchiver(segment).restore(segment);
  }

  private DataSegmentArchiver getArchiver(DataSegment segment) throws SegmentLoadingException
  {
    String type = MapUtils.getString(segment.getLoadSpec(), "type");
    DataSegmentArchiver archiver = archivers.get(type);

    if (archiver == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, archivers.keySet());
    }

    return archiver;
  }
}
