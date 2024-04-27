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

package org.apache.druid.query.aggregation.post;

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
  public static final byte ZTEST = 21;
  public static final byte PVALUE_FROM_ZTEST = 22;
  public static final byte THETA_SKETCH_CONSTANT = 23;
  public static final byte MOMENTS_SKETCH_TO_QUANTILES_CACHE_TYPE_ID = 24;
  public static final byte MOMENTS_SKETCH_TO_MIN_CACHE_TYPE_ID = 25;
  public static final byte MOMENTS_SKETCH_TO_MAX_CACHE_TYPE_ID = 26;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_RANK_CACHE_TYPE_ID = 27;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_CDF_CACHE_TYPE_ID = 28;
  public static final byte THETA_SKETCH_TO_STRING = 29;
  public static final byte TDIGEST_SKETCH_TO_QUANTILES_CACHE_TYPE_ID = 30;
  public static final byte TDIGEST_SKETCH_TO_QUANTILE_CACHE_TYPE_ID = 31;
  public static final byte HLL_SKETCH_TO_ESTIMATE_CACHE_TYPE_ID = 32;
  public static final byte KLL_DOUBLES_SKETCH_TO_RANK_CACHE_TYPE_ID = 33;
  public static final byte KLL_DOUBLES_SKETCH_TO_CDF_CACHE_TYPE_ID = 34;
  public static final byte KLL_DOUBLES_SKETCH_TO_HISTOGRAM_CACHE_TYPE_ID = 35;
  public static final byte KLL_DOUBLES_SKETCH_TO_QUANTILE_CACHE_TYPE_ID = 36;
  public static final byte KLL_DOUBLES_SKETCH_TO_QUANTILES_CACHE_TYPE_ID = 37;
  public static final byte KLL_DOUBLES_SKETCH_TO_STRING_CACHE_TYPE_ID = 38;
  public static final byte KLL_FLOATS_SKETCH_TO_RANK_CACHE_TYPE_ID = 39;
  public static final byte KLL_FLOATS_SKETCH_TO_CDF_CACHE_TYPE_ID = 40;
  public static final byte KLL_FLOATS_SKETCH_TO_HISTOGRAM_CACHE_TYPE_ID = 41;
  public static final byte KLL_FLOATS_SKETCH_TO_QUANTILE_CACHE_TYPE_ID = 42;
  public static final byte KLL_FLOATS_SKETCH_TO_QUANTILES_CACHE_TYPE_ID = 43;
  public static final byte KLL_FLOATS_SKETCH_TO_STRING_CACHE_TYPE_ID = 44;
  public static final byte SPECTATOR_HISTOGRAM_SKETCH_PERCENTILE_CACHE_TYPE_ID = 45;
  public static final byte SPECTATOR_HISTOGRAM_SKETCH_PERCENTILES_CACHE_TYPE_ID = 46;
  public static final byte DDSKETCH_QUANTILES_TYPE_ID = 51;
  public static final byte DDSKETCH_QUANTILE_TYPE_ID = 52;
}
