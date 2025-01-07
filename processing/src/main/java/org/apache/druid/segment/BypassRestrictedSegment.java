package org.apache.druid.segment;

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.policy.Policy;

/**
 * A wrapped {@link SegmentReference} with a {@link DimFilter} restriction, and the policy restriction can be bypassed.
 * <p>
 * In some methods, such as {@link #as(Class)}, {@link #asQueryableIndex()}, and {@link #asCursorFactory()}, the policy
 * is ignored.
 */
class BypassRestrictedSegment extends RestrictedSegment
{
  public BypassRestrictedSegment(SegmentReference delegate, Policy policy)
  {
    super(delegate, policy);
  }

  public Policy getPolicy()
  {
    return policy;
  }

  @Override
  public CursorFactory asCursorFactory()
  {
    return delegate.asCursorFactory();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return delegate.asQueryableIndex();
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    return delegate.as(clazz);
  }
}
