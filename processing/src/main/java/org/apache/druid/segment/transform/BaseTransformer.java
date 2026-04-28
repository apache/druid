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

package org.apache.druid.segment.transform;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface for transforming input rows during ingestion. Created by {@link BaseTransformSpec#toTransformer()}.
 *
 * @see Transformer for expression-based transforms
 * @see ScanTransformer for scan-query-based transforms
 */
public interface BaseTransformer extends Closeable
{
  /**
   * Whether this transformer can produce multiple output rows from a single input row.
   * When true, readers use {@link #transformToList} with flatMap iteration.
   * When false, readers use {@link #transform(InputRow)} with map iteration.
   */
  boolean hasMultiRowTransform();

  /**
   * Transforms a single input row, or returns null if the row should be filtered out.
   * Only called when {@link #hasMultiRowTransform()} is false.
   */
  @Nullable
  InputRow transform(@Nullable InputRow row);

  /**
   * Transforms a single input row into zero or more output rows.
   * Returns an empty list if the row is null or filtered out.
   */
  List<InputRow> transformToList(@Nullable InputRow row);

  /**
   * Transforms a batch of input rows with their associated raw values, used by the sampling path.
   * Applies transforms and filtering while maintaining the correspondence between input rows and raw values.
   */
  @Nullable
  InputRowListPlusRawValues transform(@Nullable InputRowListPlusRawValues row);

  /**
   * Releases any resources held by this transformer. The default implementation is a no-op.
   */
  @Override
  default void close() throws IOException
  {
  }
}
