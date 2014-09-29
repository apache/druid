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

package io.druid.segment;import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public interface QueryableIndex extends ColumnSelector
{
  public Interval getDataInterval();
  public int getNumRows();
  public Indexed<String> getColumnNames();
  public Indexed<String> getAvailableDimensions();

  /**
   * The close method shouldn't actually be here as this is nasty. We will adjust it in the future.
   * @throws java.io.IOException if an exception was thrown closing the index
   */
  @Deprecated
  public void close() throws IOException;
}
