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

import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.Segment;

import javax.inject.Inject;
import java.util.Map;

/**
 */
public class DelegatingSegmentLoader implements SegmentLoader
{
  private static final Logger log = new Logger(DelegatingSegmentLoader.class);

  private volatile Map<String, SegmentLoader> loaderTypes;

  @Inject
  public void setLoaderTypes(
      Map<String, SegmentLoader> loaderTypes
  )
  {
    this.loaderTypes = loaderTypes;
  }

  @Override
  public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
  {
    return getLoader(segment.getLoadSpec()).isSegmentLoaded(segment);
  }

  @Override
  public Segment getSegment(DataSegment segment) throws SegmentLoadingException
  {
    return getLoader(segment.getLoadSpec()).getSegment(segment);
  }

  @Override
  public void cleanup(DataSegment segment) throws SegmentLoadingException
  {
    getLoader(segment.getLoadSpec()).cleanup(segment);
  }

  private SegmentLoader getLoader(Map<String, Object> loadSpec) throws SegmentLoadingException
  {
    String type = MapUtils.getString(loadSpec, "type");
    SegmentLoader loader = loaderTypes.get(type);

    if (loader == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, loaderTypes.keySet());
    }
    return loader;
  }
}
