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

import com.google.common.collect.ImmutableMap;
import io.github.resilience4j.bulkhead.Bulkhead;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.RetainedLookupExtractor;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.scheduling.NoQueryLaningStrategy;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalQuerySegmentWalkerTest extends InitializedNullHandlingTest
{
  enum FailureStage
  {
    NONE,
    SETUP,
    RUN,
    CONSUME,
    SCHEDULER
  }

  @ParameterizedTest
  @EnumSource(FailureStage.class)
  public void testReleasesRetainedJoinable(final FailureStage failureStage)
  {
    final RuntimeException failure = new IllegalStateException("query failed");
    final AtomicInteger acquisitions = new AtomicInteger();
    final AtomicInteger releases = new AtomicInteger();
    final JoinableFactoryWrapper joinableFactory = new JoinableFactoryWrapper(new NoopJoinableFactory()
    {
      @Override
      public Optional<Joinable> build(final DataSource dataSource, final JoinConditionAnalysis condition)
      {
        acquisitions.incrementAndGet();
        final RetainedLookupExtractor retained = RetainedLookupExtractor.create(
            new MapLookupExtractor(ImmutableMap.of("key", "value"), false),
            releases::incrementAndGet
        );
        return Optional.of(LookupJoinable.wrap(retained, retained));
      }
    });
    final ScanQuery query = Druids.newScanQueryBuilder()
                                 .dataSource(JoinDataSource.create(
                                     InlineDataSource.fromIterable(
                                         Collections.singletonList(new Object[]{"key"}),
                                         RowSignature.builder().add("key", ColumnType.STRING).build()
                                     ),
                                     new LookupDataSource("lookup"),
                                     "j.",
                                     "key == \"j.k\"",
                                     JoinType.LEFT,
                                     null,
                                     ExprMacroTable.nil(),
                                     joinableFactory,
                                     JoinAlgorithm.BROADCAST
                                 ))
                                 .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
                                 .columns("j.v")
                                 .build();
    final ScanQueryRunnerFactory runnerFactory = new ScanQueryRunnerFactory(
        new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
        new ScanQueryEngine(),
        new ScanQueryConfig()
    )
    {
      @Override
      public QueryRunner<ScanResultValue> mergeRunners(
          final QueryProcessingPool queryProcessingPool,
          final Iterable<QueryRunner<ScanResultValue>> queryRunners
      )
      {
        if (failureStage == FailureStage.SETUP) {
          throw failure;
        }
        return (queryPlus, responseContext) -> {
          if (failureStage == FailureStage.RUN) {
            throw failure;
          }
          return new LazySequence<>(() -> {
            if (failureStage == FailureStage.CONSUME) {
              throw failure;
            }
            return Sequences.empty();
          });
        };
      }
    };
    final QueryScheduler scheduler = new QueryScheduler(
        0,
        ManualQueryPrioritizationStrategy.INSTANCE,
        NoQueryLaningStrategy.INSTANCE,
        new ServerConfig()
    )
    {
      @Override
      List<Bulkhead> acquireLanes(final Query<?> scheduledQuery)
      {
        if (failureStage == FailureStage.SCHEDULER) {
          throw failure;
        }
        return super.acquireLanes(scheduledQuery);
      }
    };
    final LocalQuerySegmentWalker walker = new LocalQuerySegmentWalker(
        DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(
            ImmutableMap.of(ScanQuery.class, runnerFactory)
        ),
        new InlineSegmentWrangler(),
        joinableFactory,
        scheduler,
        NoopPolicyEnforcer.instance(),
        new StubServiceEmitter()
    );

    if (failureStage == FailureStage.SETUP) {
      Assertions.assertSame(failure, Assertions.assertThrows(
          RuntimeException.class,
          () -> walker.getQueryRunnerForIntervals(query, Intervals.ONLY_ETERNITY)
      ));
    } else {
      final QueryRunner<ScanResultValue> runner = walker.getQueryRunnerForIntervals(query, Intervals.ONLY_ETERNITY);
      Assertions.assertEquals(1, acquisitions.get());
      Assertions.assertEquals(0, releases.get());
      if (failureStage == FailureStage.NONE) {
        Assertions.assertTrue(runner.run(QueryPlus.wrap(query), ResponseContext.createEmpty()).toList().isEmpty());
      } else {
        Assertions.assertSame(failure, Assertions.assertThrows(
            RuntimeException.class,
            () -> runner.run(QueryPlus.wrap(query), ResponseContext.createEmpty()).toList()
        ));
      }
    }
    Assertions.assertEquals(1, acquisitions.get());
    Assertions.assertEquals(1, releases.get());
  }
}
