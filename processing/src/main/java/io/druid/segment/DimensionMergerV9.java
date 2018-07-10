/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.segment.column.ColumnDescriptor;

import java.io.IOException;

/**
 * Processing related interface
 *
 * DimensionMerger subclass to be used with IndexMergerV9.
 */
public interface DimensionMergerV9<EncodedTypeArray> extends DimensionMerger<EncodedTypeArray>
{
  /**
   * Return a ColumnDescriptor containing ColumnPartSerde objects appropriate for
   * this DimensionMerger's value metadata, sequence of row values, and index structures.
   *
   * @return ColumnDescriptor that IndexMergerV9 will use to build a column.
   */
  ColumnDescriptor makeColumnDescriptor() throws IOException;
}
