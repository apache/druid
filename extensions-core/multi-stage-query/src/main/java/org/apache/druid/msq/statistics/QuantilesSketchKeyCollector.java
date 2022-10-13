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

package org.apache.druid.msq.statistics;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A key collector that is used when not aggregating. It uses a quantiles sketch to track keys.
 *
 * The collector maintains the averageKeyLength for all keys added through {@link #add(RowKey, long)} or
 * {@link #addAll(QuantilesSketchKeyCollector)}. The average is calculated as a running average and accounts for
 * weight of the key added. The averageKeyLength is assumed to be unaffected by {@link #downSample()}.
 */
public class QuantilesSketchKeyCollector implements KeyCollector<QuantilesSketchKeyCollector>
{
  private final Comparator<RowKey> comparator;
  private ItemsSketch<RowKey> sketch;
  private double averageKeyLength;

  QuantilesSketchKeyCollector(
      final Comparator<RowKey> comparator,
      @Nullable final ItemsSketch<RowKey> sketch,
      double averageKeyLength
  )
  {
    this.comparator = comparator;
    this.sketch = sketch;
    this.averageKeyLength = averageKeyLength;
  }

  @Override
  public void add(RowKey key, long weight)
  {
    double estimatedTotalSketchSizeInBytes = averageKeyLength * sketch.getN();
    // The key is added "weight" times to the sketch, we can update the total weight directly.
    estimatedTotalSketchSizeInBytes += key.getNumberOfBytes() * weight;
    for (int i = 0; i < weight; i++) {
      // Add the same key multiple times to make it "heavier".
      sketch.update(key);
    }
    averageKeyLength = (estimatedTotalSketchSizeInBytes / sketch.getN());
  }

  @Override
  public void addAll(QuantilesSketchKeyCollector other)
  {
    final ItemsUnion<RowKey> union = ItemsUnion.getInstance(
        Math.max(sketch.getK(), other.sketch.getK()),
        comparator
    );

    double sketchBytesCount = averageKeyLength * sketch.getN();
    double otherBytesCount = other.averageKeyLength * other.getSketch().getN();
    averageKeyLength = ((sketchBytesCount + otherBytesCount) / (sketch.getN() + other.sketch.getN()));

    union.update(sketch);
    union.update(other.sketch);
    sketch = union.getResultAndReset();
  }

  @Override
  public boolean isEmpty()
  {
    return sketch.isEmpty();
  }

  @Override
  public long estimatedTotalWeight()
  {
    return sketch.getN();
  }

  @Override
  public double estimatedRetainedBytes()
  {
    return averageKeyLength * estimatedRetainedKeys();
  }

  @Override
  public int estimatedRetainedKeys()
  {
    return sketch.getRetainedItems();
  }

  @Override
  public boolean downSample()
  {
    if (sketch.getN() <= 1) {
      return true;
    } else if (sketch.getK() == 2) {
      return false;
    } else {
      sketch = sketch.downSample(sketch.getK() / 2);
      return true;
    }
  }

  @Override
  public RowKey minKey()
  {
    final RowKey minValue = sketch.getMinValue();

    if (minValue != null) {
      return minValue;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetWeight(final long targetWeight)
  {
    if (targetWeight <= 0) {
      throw new IAE("targetPartitionWeight must be positive, but was [%d]", targetWeight);
    }

    if (sketch.getN() == 0) {
      return ClusterByPartitions.oneUniversalPartition();
    }

    final int numPartitions = Ints.checkedCast(LongMath.divide(sketch.getN(), targetWeight, RoundingMode.CEILING));

    // numPartitions + 1, because the final quantile is the max, and we won't build a partition based on that.
    final RowKey[] quantiles = sketch.getQuantiles(numPartitions + 1);
    final List<ClusterByPartition> partitions = new ArrayList<>();

    for (int i = 0; i < numPartitions; i++) {
      final boolean isFinalPartition = i == numPartitions - 1;

      if (isFinalPartition) {
        partitions.add(new ClusterByPartition(quantiles[i], null));
      } else {
        final ClusterByPartition partition = new ClusterByPartition(quantiles[i], quantiles[i + 1]);
        final int cmp = comparator.compare(partition.getStart(), partition.getEnd());
        if (cmp < 0) {
          // Skip partitions where start == end.
          // I don't think start can be greater than end, but if that happens, skip them too!
          partitions.add(partition);
        }
      }
    }

    return new ClusterByPartitions(partitions);
  }

  /**
   * Retrieves the backing sketch. Exists for usage by {@link QuantilesSketchKeyCollectorFactory}.
   */
  ItemsSketch<RowKey> getSketch()
  {
    return sketch;
  }

  /**
   * Retrieves the average key length. Exists for usage by {@link QuantilesSketchKeyCollectorFactory}.
   */
  double getAverageKeyLength()
  {
    return averageKeyLength;
  }
}
