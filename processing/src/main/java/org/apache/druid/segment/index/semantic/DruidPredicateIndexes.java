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

package org.apache.druid.segment.index.semantic;

import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;

import javax.annotation.Nullable;

/**
 * Uses a {@link DruidPredicateFactory} to construct a {@link BitmapColumnIndex} containing rows for all values which
 * satisfy the predicate.
 */
public interface DruidPredicateIndexes
{
  /**
   * Get a {@link BitmapColumnIndex} corresponding to all the rows that match the supplied {@link DruidPredicateFactory}
   * <p>
   * If this method returns null it indicates that there was no index that matched the respective values and a
   * {@link org.apache.druid.query.filter.ValueMatcher} must be used instead.
   */
  @Nullable
  BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory);
}
