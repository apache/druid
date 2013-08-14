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
  private final QueryRunnerFactory<T, Query<T>> factory;
  private final ReferenceCountingSegment adapter;

  public ReferenceCountingSegmentQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      ReferenceCountingSegment adapter
  )
  {
    this.factory = factory;
    this.adapter = adapter;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final Closeable closeable = adapter.increment();
    try {
      final Sequence<T> baseSequence = factory.createRunner(adapter).run(query);

      return new ResourceClosingSequence<T>(baseSequence, closeable);
    }
    catch (RuntimeException e) {
      Closeables.closeQuietly(closeable);
      throw e;
    }
  }
}
