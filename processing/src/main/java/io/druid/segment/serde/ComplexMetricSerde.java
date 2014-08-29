/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.segment.serde;

import com.google.common.base.Function;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

/**
 */
public abstract class ComplexMetricSerde
{
  public abstract String getTypeName();
  public abstract ComplexMetricExtractor getExtractor();

  /**
   * Deserializes a ByteBuffer and adds it to the ColumnBuilder.  This method allows for the ComplexMetricSerde
   * to implement it's own versioning scheme to allow for changes of binary format in a forward-compatible manner.
   *
   * The method is also in charge of returning a ColumnPartSerde that knows how to serialize out the object it
   * added to the builder.
   *
   * @param buffer the buffer to deserialize
   * @param builder ColumnBuilder to add the column to
   * @return a ColumnPartSerde that can serialize out the object that was read from the buffer to the builder
   */
  public abstract ColumnPartSerde deserializeColumn(ByteBuffer buffer, ColumnBuilder builder);

  /**
   * This is deprecated because its usage is going to be removed from the code.
   *
   * It was introduced before deserializeColumn() existed.  This method creates the assumption that Druid knows
   * how to interpret the actual column representation of the data, but I would much prefer that the ComplexMetricSerde
   * objects be in charge of creating and interpreting the whole column, which is what deserializeColumn lets
   * them do.
   *
   * @return an ObjectStrategy as used by GenericIndexed
   */
  @Deprecated
  public abstract ObjectStrategy getObjectStrategy();

  /**
   * Returns a function that can convert the Object provided by the ComplexColumn created through deserializeColumn
   * into a number of expected input bytes to produce that object.
   *
   * This is used to approximate the size of the input data via the SegmentMetadataQuery and does not need to be
   * overridden if you do not care about the query.
   *
   * @return A function that can compute the size of the complex object or null if you cannot/do not want to compute it
   */
  public Function<Object, Long> inputSizeFn()
  {
    return null;
  }
}
