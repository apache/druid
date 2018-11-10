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

package org.apache.druid.query.extraction;

/**
 *
 */
public class ExtractionCacheHelper
{
  public static final byte CACHE_TYPE_ID_TIME_DIM = 0x0;
  public static final byte CACHE_TYPE_ID_REGEX = 0x1;
  public static final byte CACHE_TYPE_ID_MATCHING_DIM = 0x2;
  public static final byte CACHE_TYPE_ID_SEARCH_QUERY = 0x3;
  public static final byte CACHE_TYPE_ID_JAVASCRIPT = 0x4;
  public static final byte CACHE_TYPE_ID_TIME_FORMAT = 0x5;
  public static final byte CACHE_TYPE_ID_IDENTITY = 0x6;
  public static final byte CACHE_TYPE_ID_LOOKUP = 0x7;
  public static final byte CACHE_TYPE_ID_SUBSTRING = 0x8;
  public static final byte CACHE_TYPE_ID_CASCADE = 0x9;
  public static final byte CACHE_TYPE_ID_STRING_FORMAT = 0xA;
  public static final byte CACHE_TYPE_ID_UPPER = 0xB;
  public static final byte CACHE_TYPE_ID_LOWER = 0xC;
  public static final byte CACHE_TYPE_ID_BUCKET = 0xD;
  public static final byte CACHE_TYPE_ID_STRLEN = 0xE;
}
