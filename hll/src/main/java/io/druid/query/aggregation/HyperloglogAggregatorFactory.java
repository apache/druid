/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import gnu.trove.map.hash.TIntByteHashMap;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class HyperloglogAggregatorFactory implements AggregatorFactory
{
  private static final Logger log = new Logger(HyperloglogAggregatorFactory.class);
  private static final byte[] CACHE_KEY = new byte[]{0x37};

  private final String name;
  private final String fieldName;

  @JsonCreator
  public HyperloglogAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new HyperloglogAggregator(
        name,
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(
      ColumnSelectorFactory metricFactory
  )
  {
    return new HyperloglogBufferAggregator(
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return HyperloglogAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    return HyperloglogAggregator.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    log.debug("factory name: %s", name);
    return new HyperloglogAggregatorFactory(name, fieldName);
  }

  @Override
  public Object deserialize(Object object)
  {
    log.debug("class name: [%s]:value [%s]", object.getClass().getName(), object);

    final String k = (String) object;
    final byte[] ibmapByte = Base64.decodeBase64(k);

    final ByteBuffer buffer = ByteBuffer.wrap(ibmapByte);
    final int keylength = buffer.getInt();
    final int valuelength = buffer.getInt();

    TIntByteHashMap newIbMap;

    if (keylength == 0) {
      newIbMap = new TIntByteHashMap();
    } else {
      final int[] keys = new int[keylength];
      final byte[] values = new byte[valuelength];

      for (int i = 0; i < keylength; i++) {
        keys[i] = buffer.getInt();
      }
      buffer.get(values);

      newIbMap = new TIntByteHashMap(keys, values);
    }

    return newIbMap;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    final TIntByteHashMap ibMap = (TIntByteHashMap) object;
    final int[] keys = ibMap.keys();
    final int count = keys.length;

    double registerSum = 0;
    double zeros = 0.0;

    for (int key : keys) {
      int val = ibMap.get(key);

      registerSum += 1.0 / (1 << val);

      if (val == 0) {
        zeros++;
      }
    }

    registerSum += (HyperloglogAggregator.m - count);
    zeros += HyperloglogAggregator.m - count;

    double estimate = HyperloglogAggregator.alphaMM * (1.0 / registerSum);

    if (estimate <= (5.0 / 2.0) * (HyperloglogAggregator.m)) {
      // Small Range Estimate
      return Math.round(HyperloglogAggregator.m * Math.log(HyperloglogAggregator.m / zeros));
    } else {
      return Math.round(estimate);
    }
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {

    byte[] fieldNameBytes = fieldName.getBytes();
    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_KEY)
                     .put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "hyperloglog";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HyperloglogAggregator.m;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return new TIntByteHashMap();
  }
}
