/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.segment;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class IndexIOTest
{
  @Test @Ignore // this test depends on static fields, so it has to be tested independently
  public void testInjector() throws Exception
  {
    System.setProperty("druid.processing.columnCache.sizeBytes", "1234");
    Assert.assertEquals(1234, IndexIO.columnConfig.columnCacheSizeBytes());
  }
}
