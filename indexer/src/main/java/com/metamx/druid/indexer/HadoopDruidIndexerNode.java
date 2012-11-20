/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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
