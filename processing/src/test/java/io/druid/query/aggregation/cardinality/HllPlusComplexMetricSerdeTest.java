package io.druid.query.aggregation.cardinality;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.hll.HyperLogLogPlus;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class HllPlusComplexMetricSerdeTest
{

  @Before
  public void setUp() throws Exception
  {
    if (ComplexMetrics.getSerdeForType("hll+") == null) {
      ComplexMetrics.registerSerde("hll+", new HllPlusComplexMetricSerde());
    }
  }

  @Test
  public void testSanity() throws Exception
  {
    IncrementalIndex rows = new IncrementalIndex(
        System.currentTimeMillis(),
        QueryGranularity.DAY,
        new AggregatorFactory[]{new DimensionCardinalityAggregatorFactory("billy", "billy")}
    );
    IncrementalIndex withDuplicate = new IncrementalIndex(
        System.currentTimeMillis(),
        QueryGranularity.DAY,
        new AggregatorFactory[]{new DimensionCardinalityAggregatorFactory("billy", "billy")}
    );

    List<String> dimensions = Lists.newArrayList("yay");

    Random random = new Random(1234);
    boolean toggle = true;
    for (int i = 0; i < 100; ++i) {
      HyperLogLogPlus hll = new HyperLogLogPlus(11);
      List<String> vals = Lists.newArrayList();
      for (int y = 0; y < random.nextInt(4) + 1; ++y) {
        String val = String.format("%d-howdy-%d", i, y);
        vals.add(val);
        hll.offer(val);
      }

      Map<String, Object> map = Maps.newHashMap();
      map.put("billy", toggle ? vals : hll);
      map.put("yay", random.nextBoolean() ? "hi" : "ho");

      MapBasedInputRow row = new MapBasedInputRow(System.currentTimeMillis(), dimensions, map);
      rows.add(row);
      withDuplicate.add(row);
      withDuplicate.add(row);
      toggle = !toggle;
    }

    Iterator<Row> singles = rows.iterator();
    Iterator<Row> duplicates = withDuplicate.iterator();

    while (singles.hasNext() && duplicates.hasNext()) {
      Row single = singles.next();
      Row duplicate = duplicates.next();

      Assert.assertEquals(single.getDimension("yay"), duplicate.getDimension("yay"));
      Assert.assertEquals(
          ((HyperLogLogPlus) single.getRaw("billy")).cardinality(),
          ((HyperLogLogPlus) duplicate.getRaw("billy")).cardinality()
      );
    }

    Assert.assertFalse("Singles should be exhausted", singles.hasNext());
    Assert.assertFalse("Duplicates should be exhausted", duplicates.hasNext());
  }
}
