/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.data;

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
  public void testNotSortedNoIndexOf()
  {
    GenericIndexed.fromArray(new String[]{"a", "c", "b"}, GenericIndexed.STRING_STRATEGY).indexOf("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSerializationNotSortedNoIndexOf() throws Exception
  {
    serializeAndDeserialize(
        GenericIndexed.fromArray(
            new String[]{"a", "c", "b"}, GenericIndexed.STRING_STRATEGY
        )
    ).indexOf("a");
  }

  @Test
  public void testSanity()
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};
    Indexed<String> indexed = GenericIndexed.fromArray(strings, GenericIndexed.STRING_STRATEGY);

    checkBasicAPIs(strings, indexed, true);

    Assert.assertEquals(-13, indexed.indexOf("q"));
    Assert.assertEquals(-9, indexed.indexOf("howdydo"));
    Assert.assertEquals(-1, indexed.indexOf("1111"));
  }

  @Test
  public void testSortedSerialization() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};

    GenericIndexed<String> deserialized = serializeAndDeserialize(
        GenericIndexed.fromArray(strings, GenericIndexed.STRING_STRATEGY)
    );

    checkBasicAPIs(strings, deserialized, true);

    Assert.assertEquals(-13, deserialized.indexOf("q"));
    Assert.assertEquals(-9, deserialized.indexOf("howdydo"));
    Assert.assertEquals(-1, deserialized.indexOf("1111"));
  }

  @Test
  public void testNotSortedSerialization() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "k", "j", "l"};

    GenericIndexed<String> deserialized = serializeAndDeserialize(
        GenericIndexed.fromArray(strings, GenericIndexed.STRING_STRATEGY)
    );
    checkBasicAPIs(strings, deserialized, false);
  }

  private void checkBasicAPIs(String[] strings, Indexed<String> index, boolean allowReverseLookup)
  {
    Assert.assertEquals(strings.length, index.size());
    for (int i = 0; i < strings.length; i++) {
      Assert.assertEquals(strings[i], index.get(i));
    }

    if (allowReverseLookup) {
      HashMap<String, Integer> mixedUp = new HashMap<>();
      for (int i = 0; i < strings.length; i++) {
        mixedUp.put(strings[i], i);
      }
      for (Map.Entry<String, Integer> entry : mixedUp.entrySet()) {
        Assert.assertEquals(entry.getValue().intValue(), index.indexOf(entry.getKey()));
      }
    } else {
      try {
        index.indexOf("xxx");
        Assert.fail("should throw exception");
      }
      catch (UnsupportedOperationException e) {
        // not supported
      }
    }
  }

  private GenericIndexed<String> serializeAndDeserialize(GenericIndexed<String> indexed) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    indexed.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(indexed.getSerializedSize(), byteBuffer.remaining());
    GenericIndexed<String> deserialized = GenericIndexed.read(byteBuffer, GenericIndexed.STRING_STRATEGY);
    Assert.assertEquals(0, byteBuffer.remaining());
    return deserialized;
  }
}
