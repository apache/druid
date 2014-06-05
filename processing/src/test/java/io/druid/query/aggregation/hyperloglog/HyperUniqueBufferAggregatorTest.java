package io.druid.query.aggregation.hyperloglog;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: neo
 * Date: 05/06/14
 * Time: 3:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class HyperUniqueBufferAggregatorTest
{
  private final HashFunction fn = Hashing.murmur3_128();
  private volatile HyperLogLogCollector collector;

  @Test
  public void testAggregation()
  {
    final HyperUniquesBufferAggregator agg = new HyperUniquesBufferAggregator(
        new ObjectColumnSelector()
        {
          @Override
          public Class classOfObject()
          {
            return HyperLogLogCollector.class;
          }

          @Override
          public Object get()
          {
            return collector;
          }
        }
    );
    ByteBuffer byteBuffer = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage());

    for (int i = 0; i < 1000; i++) {
      collector = HyperLogLogCollector.makeLatestCollector();
      collector.add(fn.hashInt(i).asBytes());
      agg.aggregate(byteBuffer, 0);
    }

    final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(
        ((HyperLogLogCollector) agg.get(
            byteBuffer,
            0
        )).toByteBuffer()
    );
    System.out.println(collector.estimateCardinality());

  }

  @Test
  public void testAggregation2()
  {
    final HyperUniquesAggregator agg = new HyperUniquesAggregator(
        "abc",
        new ObjectColumnSelector()
        {
          @Override
          public Class classOfObject()
          {
            return HyperLogLogCollector.class;
          }

          @Override
          public Object get()
          {
            return collector;
          }
        }
    );

    for (int i = 0; i < 1000; i++) {
      collector = HyperLogLogCollector.makeLatestCollector();
      collector.add(fn.hashInt(i).asBytes());
      agg.aggregate();
    }

    final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(
        ((HyperLogLogCollector) agg.get(
        )).toByteBuffer()
    );
    System.out.println(collector.estimateCardinality());

  }
}
