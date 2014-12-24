package io.druid.segment;

import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class NullDimensionSelectorTest {

  private final NullDimensionSelector selector = new NullDimensionSelector();

  @Test
  public void testGetRow() throws Exception {
    IndexedInts row = selector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));

    Iterator<Integer> iter = row.iterator();
    Assert.assertEquals(true, iter.hasNext());
    Assert.assertEquals(0, iter.next().intValue());
    Assert.assertEquals(false, iter.hasNext());
  }

  @Test
  public void testGetValueCardinality() throws Exception {
    Assert.assertEquals(1, selector.getValueCardinality());
  }

  @Test
  public void testLookupName() throws Exception {
    Assert.assertEquals(null, selector.lookupName(0));
  }

  @Test
  public void testLookupId() throws Exception {
    Assert.assertEquals(0, selector.lookupId(null));
    Assert.assertEquals(0, selector.lookupId(""));
    Assert.assertEquals(-1, selector.lookupId("billy"));
  }
}