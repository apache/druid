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

package org.apache.druid.math.expr.vector.simd;

/**
 * Identifies which unary math operations have a {@code jdk.incubator.vector} (SIMD) specialization. Used by
 * {@link org.apache.druid.math.expr.vector.SimpleVectorMathUnivariateProcessorFactory} subclasses to declare that
 * their operation can be dispatched to a SIMD variant when the user enables
 * {@link org.apache.druid.math.expr.ExpressionProcessingConfig#USE_VECTOR_API}.
 *
 * Deliberately does not reference any {@code jdk.incubator.vector} types so that callers wiring the enum into
 * factories do not need the incubator module visible.
 */
public enum SimdSupportedUnaryOp
{
  NEG,
  ABS,
  SQRT
}
