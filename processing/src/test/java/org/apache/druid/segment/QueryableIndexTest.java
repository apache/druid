package org.apache.druid.segment;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

@RunWith(Enclosed.class)
public class QueryableIndexTest
{
  public static class StringDictionaryEncodedColumnTest extends InitializedNullHandlingTest
  {
    private StringDictionaryEncodedColumn qualityColumn;
    private StringDictionaryEncodedColumn placementishColumn;
    private StringDictionaryEncodedColumn partialNullColumn;

    @Before
    public void setUp()
    {
      final QueryableIndex index = TestIndex.getMMappedTestIndex();
      qualityColumn = (StringDictionaryEncodedColumn) index.getColumnHolder("quality").getColumn();
      placementishColumn = (StringDictionaryEncodedColumn) index.getColumnHolder("placementish").getColumn();
      partialNullColumn = (StringDictionaryEncodedColumn) index.getColumnHolder("partial_null_column").getColumn();
    }

    @Test
    public void test_hasMultipleValues_quality()
    {
      Assert.assertFalse(qualityColumn.hasMultipleValues());
      Assert.assertTrue(placementishColumn.hasMultipleValues());
      Assert.assertFalse(partialNullColumn.hasMultipleValues());
    }

    @Test
    public void test_hasMultipleValues_placementish()
    {
      Assert.assertTrue(placementishColumn.hasMultipleValues());
    }

    @Test
    public void test_hasMultipleValues_partialNull()
    {
      Assert.assertFalse(partialNullColumn.hasMultipleValues());
    }

    @Test
    public void test_getCardinality_quality()
    {
      Assert.assertEquals(9, qualityColumn.getCardinality());
    }

    @Test
    public void test_getCardinality_placementish()
    {
      Assert.assertEquals(9, placementishColumn.getCardinality());
    }

    @Test
    public void test_getCardinality_partialNullColumn()
    {
      Assert.assertEquals(2, partialNullColumn.getCardinality());
    }

    @Test
    public void test_lookupName_quality()
    {
      Assert.assertEquals("automotive", qualityColumn.lookupName(0));
      Assert.assertEquals("business", qualityColumn.lookupName(1));
      Assert.assertEquals("entertainment", qualityColumn.lookupName(2));
      Assert.assertEquals("health", qualityColumn.lookupName(3));
      Assert.assertEquals("mezzanine", qualityColumn.lookupName(4));
      Assert.assertEquals("news", qualityColumn.lookupName(5));
      Assert.assertEquals("premium", qualityColumn.lookupName(6));
      Assert.assertEquals("technology", qualityColumn.lookupName(7));
      Assert.assertEquals("travel", qualityColumn.lookupName(8));
    }

    @Test
    public void test_lookupName_placementish()
    {
      Assert.assertEquals("a", placementishColumn.lookupName(0));
      Assert.assertEquals("b", placementishColumn.lookupName(1));
      Assert.assertEquals("e", placementishColumn.lookupName(2));
      Assert.assertEquals("h", placementishColumn.lookupName(3));
      Assert.assertEquals("m", placementishColumn.lookupName(4));
      Assert.assertEquals("n", placementishColumn.lookupName(5));
      Assert.assertEquals("p", placementishColumn.lookupName(6));
      Assert.assertEquals("preferred", placementishColumn.lookupName(7));
      Assert.assertEquals("t", placementishColumn.lookupName(8));
    }

    @Test
    public void test_lookupName_partialNull()
    {
      Assert.assertNull(partialNullColumn.lookupName(0));
      Assert.assertEquals("value", partialNullColumn.lookupName(1));
    }

    @Test
    public void test_lookupNameBinary_quality()
    {
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("automotive")), qualityColumn.lookupNameBinary(0));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("business")), qualityColumn.lookupNameBinary(1));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("entertainment")), qualityColumn.lookupNameBinary(2));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("health")), qualityColumn.lookupNameBinary(3));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("mezzanine")), qualityColumn.lookupNameBinary(4));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("news")), qualityColumn.lookupNameBinary(5));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("premium")), qualityColumn.lookupNameBinary(6));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("technology")), qualityColumn.lookupNameBinary(7));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("travel")), qualityColumn.lookupNameBinary(8));
    }

    @Test
    public void test_lookupNameBinary_placementish()
    {
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("a")), placementishColumn.lookupNameBinary(0));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("b")), placementishColumn.lookupNameBinary(1));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("e")), placementishColumn.lookupNameBinary(2));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("h")), placementishColumn.lookupNameBinary(3));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("m")), placementishColumn.lookupNameBinary(4));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("n")), placementishColumn.lookupNameBinary(5));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("p")), placementishColumn.lookupNameBinary(6));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("preferred")), placementishColumn.lookupNameBinary(7));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("t")), placementishColumn.lookupNameBinary(8));
    }

    @Test
    public void test_lookupNameBinary_partialNull()
    {
      Assert.assertNull(partialNullColumn.lookupNameBinary(0));
      Assert.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("value")), partialNullColumn.lookupNameBinary(1));
    }

    @Test
    public void test_lookupNameBinary_buffersAreNotShared()
    {
      // Different buffer on different calls; enables callers to safely modify position, limit as promised in
      // the javadocs.
      Assert.assertNotSame(qualityColumn.lookupNameBinary(0), qualityColumn.lookupNameBinary(0));
    }
  }
}
