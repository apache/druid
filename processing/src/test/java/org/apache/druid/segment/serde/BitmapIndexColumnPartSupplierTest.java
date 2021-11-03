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


package org.apache.druid.segment.serde;

import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.ListIndexed;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;

public class BitmapIndexColumnPartSupplierTest
{
  @Test
  public void testNonStringDictionaries()
  {
    byte[] bytes1 = new byte[]{0x01, 0x02, 0x03};
    String base64_bytes1 = StringUtils.encodeBase64String(bytes1);
    byte[] bytes2 = new byte[]{0x02, 0x03, 0x04};
    String base64_bytes2 = StringUtils.encodeBase64String(bytes2);
    MutableRoaringBitmap wrapped = new MutableRoaringBitmap();
    wrapped.add(1);
    MutableRoaringBitmap wrapped2 = new MutableRoaringBitmap();
    wrapped2.add(2);
    BitmapIndexColumnPartSupplier<TestBlob> indexSupplier = new BitmapIndexColumnPartSupplier<>(
        new RoaringBitmapFactory(),
        new ListIndexed<>(new WrappedImmutableRoaringBitmap(wrapped.toImmutableRoaringBitmap()), new WrappedImmutableRoaringBitmap(wrapped2.toImmutableRoaringBitmap())),
        new ListIndexed<>(new TestBlob(ByteBuffer.wrap(bytes1)), new TestBlob(ByteBuffer.wrap(bytes2))),
        new TestConverter()
    );

    BitmapIndex index = indexSupplier.get();

    Assert.assertEquals(base64_bytes1, index.getValue(0));
    Assert.assertEquals(0, index.getIndex(base64_bytes1));
    Assert.assertEquals(base64_bytes2, index.getValue(1));
    Assert.assertEquals(1, index.getIndex(base64_bytes2));

    Assert.assertTrue(index.getBitmap(index.getIndex(base64_bytes1)).get(1));
    Assert.assertFalse(index.getBitmap(index.getIndex(base64_bytes1)).get(2));

    Assert.assertFalse(index.getBitmap(index.getIndex(base64_bytes2)).get(1));
    Assert.assertTrue(index.getBitmap(index.getIndex(base64_bytes2)).get(2));
  }

  static class TestConverter implements BitmapIndexColumnPartSupplier.BitmapIndexConverter<TestBlob>
  {

    @Nullable
    @Override
    public TestBlob fromString(@Nullable String value)
    {
      if (value == null) {
        return null;
      }
      return TestBlob.fromString(value);
    }

    @Nullable
    @Override
    public String toString(@Nullable TestBlob value)
    {
      if (value == null) {
        return null;
      }
      return value.getString();
    }
  }
  static class TestBlob implements Comparable<TestBlob>
  {
    final ByteBuffer blob;

    TestBlob(ByteBuffer blob)
    {
      this.blob = blob;
    }

    public String getString()
    {
      byte[] bytes = new byte[blob.limit() - blob.position()];
      blob.duplicate().get(bytes);
      return StringUtils.encodeBase64String(bytes);
    }

    public static TestBlob fromString(String base64)
    {
      return new TestBlob(ByteBuffer.wrap(StringUtils.decodeBase64String(base64)));
    }

    @Override
    public int compareTo(TestBlob o)
    {
      return blob.compareTo(o.blob);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestBlob testBlob = (TestBlob) o;
      return blob.equals(testBlob.blob);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(blob);
    }
  }
}
