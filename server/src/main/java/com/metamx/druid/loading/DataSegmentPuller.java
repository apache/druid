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

import com.metamx.druid.client.DataSegment;

import java.io.File;

/**
 */
public interface DataSegmentPuller
{
  /**
   * Pull down segment files for the given DataSegment and put them in the given directory.
   *
   * @param segment The segment to pull down files for
   * @param dir The directory to store the files in
   * @throws SegmentLoadingException if there are any errors
   */
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException;

  /**
   * Returns the last modified time of the given segment.
   *
   * Note, this is not actually used at this point and doesn't need to actually be implemented.  It's just still here
   * to not break compatibility.
   *
   * @param segment The segment to check the last modified time for
   * @return the last modified time in millis from the epoch
   * @throws SegmentLoadingException if there are any errors
   */
  @Deprecated
  public long getLastModified(DataSegment segment) throws SegmentLoadingException;
}
