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

package com.metamx.druid.indexer.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.druid.index.v1.SpatialDimensionSchema;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.jackson.CommonObjectMapper;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.nio.ByteBuffer;

public class InputRowParserSerdeTest
{
  private final ObjectMapper jsonMapper = new CommonObjectMapper();

  @Test
  public void testStringInputRowParserSerde() throws Exception
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new TimestampSpec("timestamp", "iso"),
        new JSONDataSpec(
            ImmutableList.of("foo", "bar"), ImmutableList.<SpatialDimensionSchema>of()
        ),
        ImmutableList.of("baz")
    );
    final ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        ByteBufferInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ByteBuffer.wrap(
            "{\"foo\":\"x\",\"bar\":\"y\",\"qux\":\"z\",\"timestamp\":\"2000\"}".getBytes(Charsets.UTF_8)
        )
    );
    Assert.assertEquals(ImmutableList.of("foo", "bar"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
    Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
    Assert.assertEquals(new DateTime("2000").getMillis(), parsed.getTimestampFromEpoch());
  }

  @Test
  public void testMapInputRowParserSerde() throws Exception
  {
    final MapInputRowParser parser = new MapInputRowParser(
        new TimestampSpec("timestamp", "iso"),
        ImmutableList.of("foo", "bar"),
        ImmutableList.of("baz")
    );
    final MapInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        MapInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ImmutableMap.<String, Object>of(
            "foo", "x",
            "bar", "y",
            "qux", "z",
            "timestamp", "2000"
        )
    );
    Assert.assertEquals(ImmutableList.of("foo", "bar"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
    Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
    Assert.assertEquals(new DateTime("2000").getMillis(), parsed.getTimestampFromEpoch());
  }
}
