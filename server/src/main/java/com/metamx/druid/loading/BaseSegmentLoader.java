package com.metamx.druid.loading;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.Segment;

import java.util.Map;

/**
 */
public class BaseSegmentLoader implements SegmentLoader
{
  private final Map<String, DataSegmentPuller> pullers;
  private final QueryableIndexFactory factory;
  private final Supplier<SegmentLoaderConfig> config;

  @Inject
  public BaseSegmentLoader(
      Map<String, DataSegmentPuller> pullers,
      QueryableIndexFactory factory,
      Supplier<SegmentLoaderConfig> config
  )
  {
    this.pullers = pullers;
    this.factory = factory;
    this.config = config;
  }

  @Override
  public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
  {
    return getLoader(segment.getLoadSpec()).isSegmentLoaded(segment);
  }

  @Override
  public Segment getSegment(DataSegment segment) throws SegmentLoadingException
  {
    return getLoader(segment.getLoadSpec()).getSegment(segment);
  }

  @Override
  public void cleanup(DataSegment segment) throws SegmentLoadingException
  {
    getLoader(segment.getLoadSpec()).cleanup(segment);
  }

  private SegmentLoader getLoader(Map<String, Object> loadSpec) throws SegmentLoadingException
  {
    String type = MapUtils.getString(loadSpec, "type");
    DataSegmentPuller loader = pullers.get(type);

    if (loader == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, pullers.keySet());
    }

    // TODO: SingleSegmentLoader should die when Guice goes out.  The logic should just be in this class.
    return new SingleSegmentLoader(loader, factory, config.get());
  }

}
