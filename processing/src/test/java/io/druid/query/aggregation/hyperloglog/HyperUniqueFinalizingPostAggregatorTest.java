package io.druid.query.aggregation.hyperloglog;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 */
public class HyperUniqueFinalizingPostAggregatorTest
{
  private final HashFunction fn = Hashing.murmur3_128();

  @Test
  public void testCompute() throws Exception
  {
    Random random = new Random(0l);
    HyperUniqueFinalizingPostAggregator postAggregator = new HyperUniqueFinalizingPostAggregator(
        "uniques"
    );
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();
      collector.add(hashedVal);
    }

    double cardinality = (Double) postAggregator.compute(ImmutableMap.<String, Object>of("uniques", collector));

    Assert.assertTrue(cardinality == 99.37233005831612);
  }
}
