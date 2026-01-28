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

package org.apache.druid.segment;

import org.apache.druid.segment.column.ColumnDescriptor;

import java.io.IOException;
import java.util.List;

/**
 * Processing related interface
 *
 * DimensionMerger subclass to be used with IndexMergerV9.
 */
public interface DimensionMergerV9 extends DimensionMerger
{
  /**
   * Return a ColumnDescriptor containing ColumnPartSerde objects appropriate for
   * this DimensionMerger's value metadata, sequence of row values, and index structures.
   *
   * @return ColumnDescriptor that IndexMergerV9 will use to build a column.
   */
  ColumnDescriptor makeColumnDescriptor();

  /**
   * Sets this merger as the "parent" of another merger for a "projection", allowing for this merger to preserve any
   * state which might be required for the projection mergers to do their thing. This method MUST be called prior to
   * performing any merge work. Typically, this method is only implemented if
   * {@link #attachParent(DimensionMergerV9, List)} requires it.
   */
  default void markAsParent()
  {
    // do nothing
  }

  /**
   * Attaches the {@link DimensionMergerV9} of a "projection" parent column so that stuff like value dictionaries can
   * be shared between parent and child. This method is called during merging instead of {@link #writeMergedValueDictionary(List)} if
   * the parent column exists.
   * 
   * @see IndexMergerV9#makeProjections 
   */
  default void attachParent(DimensionMergerV9 parent, List<IndexableAdapter> projectionAdapters) throws IOException
  {
    // by default fall through to writing merged dictionary
    writeMergedValueDictionary(projectionAdapters);
  }
}
