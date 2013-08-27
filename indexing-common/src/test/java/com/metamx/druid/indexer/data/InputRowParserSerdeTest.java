package com.metamx.druid.indexer.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.druid.index.v1.SpatialDimensionSchema;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.jackson.DefaultObjectMapper;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.nio.ByteBuffer;

public class InputRowParserSerdeTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

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
