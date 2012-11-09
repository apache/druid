package com.metamx.druid.indexer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import org.codehaus.jackson.map.jsontype.NamedType;
import org.joda.time.Interval;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 */
public class HadoopDruidIndexerNode
{
  public static Builder builder()
  {
    return new Builder();
  }

  private String intervalSpec = null;
  private String argumentSpec = null;

  public String getIntervalSpec()
  {
    return intervalSpec;
  }

  public String getArgumentSpec()
  {
    return argumentSpec;
  }

  public HadoopDruidIndexerNode setIntervalSpec(String intervalSpec)
  {
    this.intervalSpec = intervalSpec;
    return this;
  }

  public HadoopDruidIndexerNode setArgumentSpec(String argumentSpec)
  {
    this.argumentSpec = argumentSpec;
    return this;
  }

  @SuppressWarnings("unchecked")
  public HadoopDruidIndexerNode registerJacksonSubtype(Class<?>... clazzes)
  {
    HadoopDruidIndexerConfig.jsonMapper.registerSubtypes(clazzes);
    return this;
  }

  @SuppressWarnings("unchecked")
  public HadoopDruidIndexerNode registerJacksonSubtype(NamedType... namedTypes)
  {
    HadoopDruidIndexerConfig.jsonMapper.registerSubtypes(namedTypes);
    return this;
  }

  @LifecycleStart
  public void start() throws Exception
  {
    Preconditions.checkNotNull(argumentSpec, "argumentSpec");

    final HadoopDruidIndexerConfig config;
    if (argumentSpec.startsWith("{")) {
      config = HadoopDruidIndexerConfig.fromString(argumentSpec);
    } else {
      config = HadoopDruidIndexerConfig.fromFile(new File(argumentSpec));
    }

    if (intervalSpec != null) {
      final List<Interval> dataInterval = Lists.transform(
          Arrays.asList(intervalSpec.split(",")),
          new StringIntervalFunction()
      );

      config.setIntervals(dataInterval);
    }

    new HadoopDruidIndexerJob(config).run();
  }

  @LifecycleStop
  public void stop()
  {
  }

  public static class Builder
  {
    public HadoopDruidIndexerNode build()
    {
      return new HadoopDruidIndexerNode();
    }
  }
}
