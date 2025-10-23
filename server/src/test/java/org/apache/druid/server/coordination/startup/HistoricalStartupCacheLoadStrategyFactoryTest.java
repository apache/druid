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

package org.apache.druid.server.coordination.startup;

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class HistoricalStartupCacheLoadStrategyFactoryTest
{
  @Test
  public void testConstructionOfEagerLoadingBeforePeriodFromConfig()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig()
    {
      @Override
      public String getStartupCacheLoadStrategy()
      {
        return LoadEagerlyBeforePeriod.STRATEGY_NAME;
      }

      @Override
      public Period getStartupLoadPeriod()
      {
        return Period.days(7);
      }
    };

    HistoricalStartupCacheLoadStrategy strategy = HistoricalStartupCacheLoadStrategyFactory.factorize(config);
    Assert.assertEquals(LoadEagerlyBeforePeriod.class, strategy.getClass());
    Assert.assertEquals(
        config.getStartupLoadPeriod().toStandardDuration(),
        ((LoadEagerlyBeforePeriod) strategy).getEagerLoadingWindow().toDuration()
    );
  }

  @Test
  public void testConstructionOfLazyLoadingFromConfig()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig()
    {
      @Override
      public String getStartupCacheLoadStrategy()
      {
        return LoadAllLazilyStrategy.STRATEGY_NAME;
      }

      // This config is not applicable for lazy / eager.
      @Override
      public Period getStartupLoadPeriod()
      {
        return Period.days(7);
      }
    };

    HistoricalStartupCacheLoadStrategy strategy = HistoricalStartupCacheLoadStrategyFactory.factorize(config);
    Assert.assertEquals(LoadAllLazilyStrategy.class, strategy.getClass());
  }

  @Test
  public void testConstructionOfEagerLoadingFromConfig()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig()
    {
      @Override
      public String getStartupCacheLoadStrategy()
      {
        return LoadAllEagerlyStrategy.STRATEGY_NAME;
      }

      // This config is not applicable for lazy / eager.
      @Override
      public Period getStartupLoadPeriod()
      {
        return Period.days(7);
      }
    };

    HistoricalStartupCacheLoadStrategy strategy = HistoricalStartupCacheLoadStrategyFactory.factorize(config);
    Assert.assertEquals(LoadAllEagerlyStrategy.class, strategy.getClass());
  }

  @Test
  public void testDefaultConstruction()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig();

    HistoricalStartupCacheLoadStrategy strategy = HistoricalStartupCacheLoadStrategyFactory.factorize(config);
    Assert.assertEquals(LoadAllEagerlyStrategy.class, strategy.getClass());
  }

  /**
   * Test to be removed when {@link SegmentLoaderConfig#isLazyLoadOnStart()} is removed.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testConfigWhenLazyLoadSetToTrue()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig()
    {
      @Override
      public boolean isLazyLoadOnStart()
      {
        return true;
      }
    };

    HistoricalStartupCacheLoadStrategy strategy = HistoricalStartupCacheLoadStrategyFactory.factorize(config);
    Assert.assertEquals(LoadAllLazilyStrategy.class, strategy.getClass());
  }

  @Test
  public void testConstructionOfInvalidConfig()
  {
    String unknownStrategy = "some unknown strategy";
    final SegmentLoaderConfig config = new SegmentLoaderConfig()
    {
      @Override
      public String getStartupCacheLoadStrategy()
      {
        return unknownStrategy;
      }
    };

    Assert.assertThrows(
        "Unknown configured Historical Startup Loading Strategy[" + unknownStrategy + "]",
        DruidException.class,
        () -> HistoricalStartupCacheLoadStrategyFactory.factorize(config)
    );
  }
}
