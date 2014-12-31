/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

package io.druid.segment.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class BitmapSerdeFactoryTest
{
  @Test
  public void testSerialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals("{\"type\":\"roaring\"}", mapper.writeValueAsString(new RoaringBitmapSerdeFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(new ConciseBitmapSerdeFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(BitmapSerde.createLegacyFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(new BitmapSerde.DefaultBitmapSerdeFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(new BitmapSerde.LegacyBitmapSerdeFactory()));
  }

  @Test
  public void testDeserialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertTrue(mapper.readValue("{\"type\":\"roaring\"}", BitmapSerdeFactory.class) instanceof RoaringBitmapSerdeFactory);
    Assert.assertTrue(mapper.readValue("{\"type\":\"concise\"}", BitmapSerdeFactory.class) instanceof ConciseBitmapSerdeFactory);
    Assert.assertTrue(mapper.readValue("{\"type\":\"BitmapSerde$SomeRandomClass\"}", BitmapSerdeFactory.class) instanceof ConciseBitmapSerdeFactory);
  }
}
