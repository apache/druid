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

/**
 */
public class OmniDataSegmentKiller implements DataSegmentKiller
{
  private final Map<String, DataSegmentKiller> killers;

  @Inject
  public OmniDataSegmentKiller(
      Map<String, DataSegmentKiller> killers
  )
  {
    this.killers = killers;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    getKiller(segment).kill(segment);
  }

  private DataSegmentKiller getKiller(DataSegment segment) throws SegmentLoadingException
  {
    String type = MapUtils.getString(segment.getLoadSpec(), "type");
    DataSegmentKiller loader = killers.get(type);

    if (loader == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, killers.keySet());
    }

    return loader;
  }

}
