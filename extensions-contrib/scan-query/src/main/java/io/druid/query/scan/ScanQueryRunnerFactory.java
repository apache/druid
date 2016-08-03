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
package io.druid.query.scan;

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.Segment;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ScanQueryRunnerFactory implements QueryRunnerFactory<ScanResultValue, ScanQuery>
{
  private final ScanQueryQueryToolChest toolChest;
  private final ScanQueryEngine engine;

  @Inject
  public ScanQueryRunnerFactory(
      ScanQueryQueryToolChest toolChest,
      ScanQueryEngine engine
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
  }

  @Override
  public QueryRunner<ScanResultValue> createRunner(Segment segment)
  {
    return new ScanQueryRunner(engine, segment);
  }

  @Override
  public QueryRunner<ScanResultValue> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<ScanResultValue>> queryRunners
  )
  {
    // in single thread and in jetty thread instead of processing thread
    return new ConcatQueryRunner<>(Sequences.simple(queryRunners));
  }

  @Override
  public QueryToolChest<ScanResultValue, ScanQuery> getToolchest()
  {
    return toolChest;
  }

  private class ScanQueryRunner implements QueryRunner<ScanResultValue>
  {
    private final ScanQueryEngine engine;
    private final Segment segment;

    public ScanQueryRunner(ScanQueryEngine engine, Segment segment)
    {
      this.engine = engine;
      this.segment = segment;
    }

    @Override
    public Sequence<ScanResultValue> run(
        Query<ScanResultValue> query, Map<String, Object> responseContext
    )
    {
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }

      return engine.process((ScanQuery) query, segment);
    }
  }
}
