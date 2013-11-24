package io.druid.query.aggregation.cardinality;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

/**
 *
 */
public class DimensionCardinalityAggregator implements Aggregator
{
  private static final Logger log = new Logger(DimensionCardinalityAggregator.class);

  static final int MAX_SIZE_BYTES = 1381;

  static final HyperLogLogPlus makeHllPlus()
  {
    return new HyperLogLogPlus(11, 0);
  }

  static final HyperLogLogPlus fromBytes(byte[] bytes)
  {
    try {
      HyperLogLogPlus retVal = makeHllPlus();
      log.info("retVal sizeof[%s], p[%s], sp[%s]", retVal.sizeof(), getField(retVal, "p"), getField(retVal, "sp"));
      HyperLogLogPlus fromBytes = HyperLogLogPlus.Builder.build(bytes);
      log.info("fromBytes sizeof[%s], p[%s], sp[%s]", fromBytes.sizeof(), getField(fromBytes, "p"), getField(fromBytes, "sp"));
      retVal.addAll(fromBytes);
      return retVal;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } catch (CardinalityMergeException e) {
      throw Throwables.propagate(e);
    }
  }

  static final Object getField(HyperLogLogPlus hll, String field)
  {
    try {
      Field declaredField = hll.getClass().getDeclaredField(field);
      declaredField.setAccessible(true);
      return declaredField.get(hll);
    } catch (NoSuchFieldException e) {
      throw Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }

  }

  private final String name;
  private final ObjectColumnSelector selector;

  private volatile HyperLogLogPlus hllPlus = null;

  public DimensionCardinalityAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
    this.selector = selector;

    this.hllPlus = makeHllPlus();
  }

  @Override
  public void aggregate()
  {
    Object obj = selector.get();
    if (obj == null) {
      hllPlus.offer(obj);
    }
    else if (obj instanceof List) {
      for (Object o : (List) obj) {
        hllPlus.offer(o);
      }
    }
    else if (obj instanceof HyperLogLogPlus) {
      try {
        hllPlus.addAll((HyperLogLogPlus) obj);
      } catch (CardinalityMergeException e) {
        throw Throwables.propagate(e);
      }
    } else if (obj instanceof String) {
      hllPlus.offer(obj);
    } else {
      throw new UnsupportedOperationException(String.format("Unexpected object type[%s].", obj.getClass()));
    }
  }

  @Override
  public void reset()
  {
    hllPlus = makeHllPlus();
  }

  @Override
  public Object get()
  {
    return hllPlus;
  }

  @Override
  public float getFloat()
  {
    return hllPlus.cardinality();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {

  }
}
