package com.metamx.druid.query;

import com.google.common.io.Closeables;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.druid.index.ReferenceCountingSegment;

import java.io.Closeable;

/**
*/
public class ReferenceCountingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> runner;
  private final ReferenceCountingSegment adapter;

  public ReferenceCountingSegmentQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      ReferenceCountingSegment adapter
  )
  {
    this.adapter = adapter;

    this.runner = factory.createRunner(adapter);
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final Closeable closeable = adapter.increment();
    try {
      final Sequence<T> baseSequence = runner.run(query);

      return new ResourceClosingSequence<T>(baseSequence, closeable);
    }
    catch (RuntimeException e) {
      Closeables.closeQuietly(closeable);
      throw e;
    }
  }
}
