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

package org.apache.druid.java.util.common.granularity;

/**
 * This class was created b/c sometimes static initializers of a class that use a subclass can deadlock.
 * See: #2979, #3979
 */
public class Granularities
{
  public static final Granularity SECOND = GranularityType.SECOND.getDefaultGranularity();
  public static final Granularity MINUTE = GranularityType.MINUTE.getDefaultGranularity();
  public static final Granularity FIVE_MINUTE = GranularityType.FIVE_MINUTE.getDefaultGranularity();
  public static final Granularity TEN_MINUTE = GranularityType.TEN_MINUTE.getDefaultGranularity();
  public static final Granularity FIFTEEN_MINUTE = GranularityType.FIFTEEN_MINUTE.getDefaultGranularity();
  public static final Granularity THIRTY_MINUTE = GranularityType.THIRTY_MINUTE.getDefaultGranularity();
  public static final Granularity HOUR = GranularityType.HOUR.getDefaultGranularity();
  public static final Granularity SIX_HOUR = GranularityType.SIX_HOUR.getDefaultGranularity();
  public static final Granularity DAY = GranularityType.DAY.getDefaultGranularity();
  public static final Granularity WEEK = GranularityType.WEEK.getDefaultGranularity();
  public static final Granularity MONTH = GranularityType.MONTH.getDefaultGranularity();
  public static final Granularity QUARTER = GranularityType.QUARTER.getDefaultGranularity();
  public static final Granularity YEAR = GranularityType.YEAR.getDefaultGranularity();
  public static final Granularity ALL = GranularityType.ALL.getDefaultGranularity();
  public static final Granularity NONE = GranularityType.NONE.getDefaultGranularity();

  public static Granularity nullToAll(Granularity granularity)
  {
    return granularity == null ? Granularities.ALL : granularity;
  }
}
