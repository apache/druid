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

package org.apache.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.server.initialization.ServerConfig;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class SetAndVerifyContextQueryRunnerTest
{
  @Test
  public void testTimeoutIsUsedIfTimeoutIsNonZero() throws InterruptedException
  {
    Query<ScanResultValue> query = new Druids.ScanQueryBuilder()
        .dataSource("foo")
        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
        .context(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1))
        .build();

    ServerConfig defaultConfig = new ServerConfig();

    QueryRunner<ScanResultValue> mockRunner = EasyMock.createMock(QueryRunner.class);
    SetAndVerifyContextQueryRunner<ScanResultValue> queryRunner = new SetAndVerifyContextQueryRunner<>(defaultConfig, mockRunner);

    Query<ScanResultValue> transformed = queryRunner.withTimeoutAndMaxScatterGatherBytes(query, defaultConfig);

    Thread.sleep(100);
    // timeout is set to 1, so withTimeoutAndMaxScatterGatherBytes should set QUERY_FAIL_TIME to be the current
    // time + 1 at the time the method was called
    // this means that after sleeping for 1 millis, the fail time should be less than the current time when checking
    Assert.assertTrue(
        System.currentTimeMillis() > transformed.context().getLong(DirectDruidClient.QUERY_FAIL_TIME)
    );
  }

  @Test
  public void testTimeoutDefaultTooBigAndOverflows()
  {
    Query<ScanResultValue> query = new Druids.ScanQueryBuilder()
        .dataSource("foo")
        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
        .build();

    ServerConfig defaultConfig = new ServerConfig()
    {
      @Override
      public long getDefaultQueryTimeout()
      {
        return Long.MAX_VALUE;
      }
    };

    QueryRunner<ScanResultValue> mockRunner = EasyMock.createMock(QueryRunner.class);
    SetAndVerifyContextQueryRunner<ScanResultValue> queryRunner = new SetAndVerifyContextQueryRunner<>(defaultConfig, mockRunner);

    Query<ScanResultValue> transformed = queryRunner.withTimeoutAndMaxScatterGatherBytes(query, defaultConfig);

    // timeout is not set, default timeout has been set to long.max, make sure timeout is still in the future
    Assert.assertEquals(Long.MAX_VALUE, (long) transformed.context().getLong(DirectDruidClient.QUERY_FAIL_TIME));
  }

  @Test
  public void testTimeoutZeroIsNotImmediateTimeoutDefaultServersideMax()
  {
    Query<ScanResultValue> query = new Druids.ScanQueryBuilder()
        .dataSource("foo")
        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
        .context(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 0))
        .build();

    ServerConfig defaultConfig = new ServerConfig();

    QueryRunner<ScanResultValue> mockRunner = EasyMock.createMock(QueryRunner.class);
    SetAndVerifyContextQueryRunner<ScanResultValue> queryRunner = new SetAndVerifyContextQueryRunner<>(defaultConfig, mockRunner);

    Query<ScanResultValue> transformed = queryRunner.withTimeoutAndMaxScatterGatherBytes(query, defaultConfig);

    // timeout is set to 0, so withTimeoutAndMaxScatterGatherBytes should set QUERY_FAIL_TIME to be the current
    // time + max query timeout at the time the method was called
    // since default is long max, expect long max since current time would overflow
    Assert.assertEquals(Long.MAX_VALUE, (long) transformed.context().getLong(DirectDruidClient.QUERY_FAIL_TIME));
  }

  @Test
  public void testTimeoutZeroIsNotImmediateTimeoutExplicitServersideMax()
  {
    Query<ScanResultValue> query = new Druids.ScanQueryBuilder()
        .dataSource("foo")
        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
        .context(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 0))
        .build();

    ServerConfig defaultConfig = new ServerConfig()
    {
      @Override
      public long getMaxQueryTimeout()
      {
        return 10000L;
      }
    };

    QueryRunner<ScanResultValue> mockRunner = EasyMock.createMock(QueryRunner.class);
    SetAndVerifyContextQueryRunner<ScanResultValue> queryRunner = new SetAndVerifyContextQueryRunner<>(defaultConfig, mockRunner);

    Query<ScanResultValue> transformed = queryRunner.withTimeoutAndMaxScatterGatherBytes(query, defaultConfig);

    // timeout is set to 0, so withTimeoutAndMaxScatterGatherBytes should set QUERY_FAIL_TIME to be the current
    // time + max query timeout at the time the method was called
    // this means that the fail time should be greater than the current time when checking
    Assert.assertTrue(
        System.currentTimeMillis() < transformed.context().getLong(DirectDruidClient.QUERY_FAIL_TIME)
    );
  }
}
