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

package io.druid.query.aggregation.post;

public class PostAggregatorIds
{
  public static final byte ARITHMETIC = 0;
  public static final byte CONSTANT = 1;
  public static final byte DOUBLE_GREATEST = 2;
  public static final byte DOUBLE_LEAST = 3;
  public static final byte EXPRESSION = 4;
  public static final byte FIELD_ACCESS = 5;
  public static final byte JAVA_SCRIPT = 6;
  public static final byte LONG_GREATEST = 7;
  public static final byte LONG_LEAST = 8;
  public static final byte HLL_HYPER_UNIQUE_FINALIZING = 9;
  public static final byte HISTOGRAM_BUCKETS = 10;
  public static final byte HISTOGRAM_CUSTOM_BUCKETS = 11;
  public static final byte HISTOGRAM_EQUAL_BUCKETS = 12;
  public static final byte HISTOGRAM_MAX = 13;
  public static final byte HISTOGRAM_MIN = 14;
  public static final byte HISTOGRAM_QUANTILE = 15;
  public static final byte HISTOGRAM_QUANTILES = 16;
  public static final byte DATA_SKETCHES_SKETCH_ESTIMATE = 17;
  public static final byte DATA_SKETCHES_SKETCH_SET = 18;
  public static final byte VARIANCE_STANDARD_DEVIATION = 19;
  public static final byte FINALIZING_FIELD_ACCESS = 20;
}
