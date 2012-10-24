package com.metamx.druid.query.segment;

import com.metamx.druid.Query;
import com.metamx.druid.query.QueryRunner;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.Interval;

import java.util.List;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type", defaultImpl = LegacySegmentSpec.class)
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="intervals", value=MultipleIntervalSegmentSpec.class),
    @JsonSubTypes.Type(name="segments", value=MultipleSpecificSegmentSpec.class)
})
public interface QuerySegmentSpec
{
  public List<Interval> getIntervals();

  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker);
}
