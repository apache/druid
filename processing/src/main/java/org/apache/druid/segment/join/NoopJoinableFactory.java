package org.apache.druid.segment.join;

import org.apache.druid.query.DataSource;

import java.util.Optional;

public class NoopJoinableFactory implements JoinableFactory
{
  public static final NoopJoinableFactory INSTANCE = new NoopJoinableFactory();

  protected NoopJoinableFactory()
  {
    // Singleton.
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    return false;
  }

  @Override
  public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
  {
    return Optional.empty();
  }
}