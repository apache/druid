package io.druid.query.aggregation.cardinality;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 *
 */
public class DimensionCardinalityBufferAggregatorTest
{
  @Test
  public void testSanity() throws Exception
  {
    final Object[] val = new Object[]{0};

    DimensionCardinalityBufferAggregator agg = new DimensionCardinalityBufferAggregator(new ObjectColumnSelector()
    {
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public Object get()
      {
        HyperLogLogPlus retVal = DimensionCardinalityAggregator.makeHllPlus();
        retVal.offer(val[0]);
        return retVal;
      }
    });

    int offset = 1029;
    ByteBuffer buf = ByteBuffer.allocate(new DimensionCardinalityAggregatorFactory("", "").getMaxIntermediateSize() + offset);
    agg.init(buf, offset);
    for (int i = 0; i < 1000; ++i) {
      val[0] = i;
      agg.aggregate(buf, offset);
    }
    val[0] = null;
    agg.aggregate(buf, offset);
    Assert.assertEquals(1023, ((HyperLogLogPlus) agg.get(buf, offset)).cardinality());
  }
}
