package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.junit.Assert;
import org.junit.Test;


public class StringDimensionIndexerTest
{
  @Test
  public void testInMemoryBitmap()
  {
    // Test update in-memory bitmap
    StringDimensionIndexer stringDimensionIndexer = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), true, false, true);
    stringDimensionIndexer.fillInMemoryBitmapsFromUnsortedEncodedKeyComponent(new int[]{1, 3}, 10, new RoaringBitmapFactory());
    stringDimensionIndexer.fillInMemoryBitmapsFromUnsortedEncodedKeyComponent(new int[]{3}, 11, new RoaringBitmapFactory());

    // Test get in-memory bitmap
    ImmutableBitmap bitmap = stringDimensionIndexer.getBitmap(1);
    Assert.assertEquals(1, bitmap.size());
    Assert.assertTrue(bitmap.get(10));

    bitmap = stringDimensionIndexer.getBitmap(3);
    Assert.assertEquals(2, bitmap.size());
    Assert.assertTrue(bitmap.get(10));
    Assert.assertTrue(bitmap.get(11));

    // Test in bound but not existing
    bitmap = stringDimensionIndexer.getBitmap(0);
    Assert.assertEquals(0, bitmap.size());
    bitmap = stringDimensionIndexer.getBitmap(2);
    Assert.assertEquals(0, bitmap.size());

    // Test out of bound
    bitmap = stringDimensionIndexer.getBitmap(4);
    Assert.assertEquals(0, bitmap.size());
  }

  @Test
  public void testInMemoryBitmapDisabled()
  {
    // Test bitmap index disabled in schema
    StringDimensionIndexer bitmapIndexDisabledInSchema = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), false, false, true);
    Assert.assertThrows(
        StringDimensionIndexer.BITMAP_INDEX_DISABLED_IN_SCHEMA_ERR_MSG,
        UnsupportedOperationException.class,
        () -> bitmapIndexDisabledInSchema.fillInMemoryBitmapsFromUnsortedEncodedKeyComponent(
            new int[1],
            0,
            new RoaringBitmapFactory()
        )
    );
    Assert.assertThrows(
        StringDimensionIndexer.BITMAP_INDEX_DISABLED_IN_SCHEMA_ERR_MSG,
        UnsupportedOperationException.class,
        () -> bitmapIndexDisabledInSchema.getBitmap(0)
    );

    // Test in-memory bitmap index disabled
    StringDimensionIndexer inMemoryBitmapIndexDisabled = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), true, false, false);
    Assert.assertThrows(
        StringDimensionIndexer.IN_MEMORY_BITMAP_INDEX_DISABLED_ERR_MSG,
        UnsupportedOperationException.class,
        () -> inMemoryBitmapIndexDisabled.fillInMemoryBitmapsFromUnsortedEncodedKeyComponent(
            new int[1],
            1,
            new RoaringBitmapFactory()
        )
    );
    Assert.assertThrows(
        StringDimensionIndexer.IN_MEMORY_BITMAP_INDEX_DISABLED_ERR_MSG,
        UnsupportedOperationException.class,
        () -> inMemoryBitmapIndexDisabled.getBitmap(0)
    );
  }
}