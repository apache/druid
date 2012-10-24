package com.metamx.druid.kv;

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
    GenericIndexed<String> deserialized = GenericIndexed.readFromByteBuffer(
        byteBuffer, GenericIndexed.stringStrategy
    );
    Assert.assertEquals(0, byteBuffer.remaining());
    return deserialized;
  }
}
