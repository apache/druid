/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Assert;
import org.junit.Test;

import io.druid.java.util.common.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 */
public class SortableBytesTest
{
  @Test
  public void testSanity() throws Exception
  {
    SortableBytes thingie1 = new SortableBytes(toBytes("test1"), toBytes("a"));
    SortableBytes thingie2 = new SortableBytes(toBytes("test1"), toBytes("b"));

    Assert.assertEquals("test1", fromBytes(thingie1.getGroupKey()));
    Assert.assertEquals("a", fromBytes(thingie1.getSortKey()));
    Assert.assertEquals("test1", fromBytes(thingie2.getGroupKey()));
    Assert.assertEquals("b", fromBytes(thingie2.getSortKey()));

    byte[] thingie1Bytes = getByteArrayBytes(thingie1);
    byte[] thingie2Bytes = getByteArrayBytes(thingie2);

    Assert.assertEquals(
        -1,
        WritableComparator.compareBytes(
            thingie1Bytes, 0, thingie1Bytes.length,
            thingie2Bytes, 0, thingie2Bytes.length
        )
    );

    Assert.assertEquals(
        0,
        new SortableBytes.SortableBytesGroupingComparator().compare(
            thingie1Bytes, 0, thingie1Bytes.length,
            thingie2Bytes, 0, thingie2Bytes.length
        )
    );

    SortableBytes reconThingie1 = SortableBytes.fromBytes(thingie1Bytes, 4, thingie1Bytes.length - 4);
    SortableBytes reconThingie2 = SortableBytes.fromBytes(thingie2Bytes, 4, thingie2Bytes.length - 4);

    Assert.assertEquals("test1", fromBytes(reconThingie1.getGroupKey()));
    Assert.assertEquals("a", fromBytes(reconThingie1.getSortKey()));
    Assert.assertEquals("test1", fromBytes(reconThingie2.getGroupKey()));
    Assert.assertEquals("b", fromBytes(reconThingie2.getSortKey()));

    thingie1Bytes = reconThingie1.toBytes();
    thingie2Bytes = reconThingie2.toBytes();

    byte[] someBytes = new byte[thingie1Bytes.length + thingie2Bytes.length + thingie1Bytes.length];
    System.arraycopy(thingie1Bytes, 0, someBytes, 0, thingie1Bytes.length);
    System.arraycopy(thingie2Bytes, 0, someBytes, thingie1Bytes.length, thingie2Bytes.length);
    System.arraycopy(thingie1Bytes, 0, someBytes, thingie1Bytes.length + thingie2Bytes.length, thingie1Bytes.length);

    reconThingie2 = SortableBytes.fromBytes(someBytes, thingie1Bytes.length, thingie2Bytes.length);
    Assert.assertEquals("test1", fromBytes(reconThingie2.getGroupKey()));
    Assert.assertEquals("b", fromBytes(reconThingie2.getSortKey()));
  }

  @Test
  public void testSortComparator() throws Exception
  {
    SortableBytes thingie1 = new SortableBytes(toBytes("test1"), toBytes("3"));
    SortableBytes thingie2 = new SortableBytes(toBytes("test12"), toBytes("b"));

    SortableBytes.SortableBytesSortingComparator comparator = new SortableBytes.SortableBytesSortingComparator();

    final byte[] thingie1Bytes = getByteArrayBytes(thingie1);
    final byte[] thingie2Bytes = getByteArrayBytes(thingie2);

    Assert.assertEquals(-1, comparator.compare(thingie1Bytes, 0, thingie1Bytes.length, thingie2Bytes, 0, thingie2Bytes.length));
  }

  private byte[] getByteArrayBytes(SortableBytes thingie1) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    thingie1.toBytesWritable().write(out);
    out.flush();
    return baos.toByteArray();
  }

  private byte[] toBytes(String string)
  {
    return StringUtils.toUtf8(string);
  }

  private String fromBytes(byte[] bytes)
  {
    return StringUtils.fromUtf8(bytes);
  }
}
