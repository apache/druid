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

package io.druid.segment.data;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class GenericIndexedTest
{
  @Test(expected = UnsupportedOperationException.class)
  public void testNotSortedNoIndexOf() throws Exception
  {
    GenericIndexed.fromArray(new String[]{"a", "c", "b"}, GenericIndexed.stringStrategy).indexOf("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSerializationNotSortedNoIndexOf() throws Exception
  {
    serializeAndDeserialize(
        GenericIndexed.fromArray(
            new String[]{"a", "c", "b"}, GenericIndexed.stringStrategy
        )
    ).indexOf("a");
  }

  @Test
  public void testSanity() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};
    Indexed<String> indexed = GenericIndexed.fromArray(strings, GenericIndexed.stringStrategy);

    Assert.assertEquals(strings.length, indexed.size());
    for (int i = 0; i < strings.length; i++) {
      Assert.assertEquals(strings[i], indexed.get(i));
    }

    HashMap<String, Integer> mixedUp = Maps.newHashMap();
    for (int i = 0; i < strings.length; i++) {
      mixedUp.put(strings[i], i);
    }

    for (Map.Entry<String, Integer> entry : mixedUp.entrySet()) {
      Assert.assertEquals(entry.getValue().intValue(), indexed.indexOf(entry.getKey()));
    }

    Assert.assertEquals(-13, indexed.indexOf("q"));
    Assert.assertEquals(-9, indexed.indexOf("howdydo"));
    Assert.assertEquals(-1, indexed.indexOf("1111"));
  }

  @Test
  public void testSortedSerialization() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};

    GenericIndexed<String> deserialized = serializeAndDeserialize(
        GenericIndexed.fromArray(
            strings, GenericIndexed.stringStrategy
        )
    );

    Assert.assertEquals(strings.length, deserialized.size());
    for (int i = 0; i < strings.length; i++) {
      Assert.assertEquals(strings[i], deserialized.get(i));
    }

    HashMap<String, Integer> mixedUp = Maps.newHashMap();
    for (int i = 0; i < strings.length; i++) {
      mixedUp.put(strings[i], i);
    }

    for (Map.Entry<String, Integer> entry : mixedUp.entrySet()) {
      Assert.assertEquals(entry.getValue().intValue(), deserialized.indexOf(entry.getKey()));
    }

    Assert.assertEquals(-13, deserialized.indexOf("q"));
    Assert.assertEquals(-9, deserialized.indexOf("howdydo"));
    Assert.assertEquals(-1, deserialized.indexOf("1111"));
  }

  private GenericIndexed<String> serializeAndDeserialize(GenericIndexed<String> indexed) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    indexed.writeToChannel(channel);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(indexed.getSerializedSize(), byteBuffer.remaining());
    GenericIndexed<String> deserialized = GenericIndexed.read(
        byteBuffer, GenericIndexed.stringStrategy
    );
    Assert.assertEquals(0, byteBuffer.remaining());
    return deserialized;
  }
}
