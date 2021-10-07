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

package org.apache.druid.segment.column;


/**
 * This is a bridge between {@link ValueType} and {@link org.apache.druid.math.expr.ExprType}, so that they can both
 * be used with {@link TypeSignature}.
 *
 * This interface assists in classifying the specific enum values that implement this interface into the current type
 * families: 'primitives', 'arrays', and 'complex' types through {@link #isPrimitive()} and {@link #isArray()}
 *
 * At some point we should probably consider reworking this into a 'TypeFamily' enum that maps these high level families
 * to specific types.
 *
 * This interface can potentially be removed if the expression processing system is updated to use {@link ColumnType}
 * instead of {@link org.apache.druid.math.expr.ExpressionType}, which would allow
 * {@link org.apache.druid.math.expr.ExprType} to be removed and this interface merged into {@link ValueType} (along
 * with consolidation of several other interfaces, see {@link TypeSignature} javadoc for additional details).
 */
public interface TypeDescriptor
{
  /**
   * Scalar numeric primitive values.
   *
   * @see ValueType#isNumeric
   * @see org.apache.druid.math.expr.ExprType#isNumeric
   */
  boolean isNumeric();

  /**
   * Scalar numeric and string values. This does not currently include complex types.
   *
   * @see ValueType#isPrimitive
   * @see org.apache.druid.math.expr.ExprType#isPrimitive
   */
  boolean isPrimitive();

  /**
   * Value is an array of some other {@link TypeDescriptor}
   *
   * @see ValueType#isArray
   * @see org.apache.druid.math.expr.ExprType#isArray
   */
  boolean isArray();
}
