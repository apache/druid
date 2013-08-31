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

package io.druid.indexer;

import com.google.common.base.Charsets;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Assert;
import org.junit.Test;

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
    return string.getBytes(Charsets.UTF_8);
  }

  private String fromBytes(byte[] bytes)
  {
    return new String(bytes, Charsets.UTF_8);
  }
}
