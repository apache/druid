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

package com.metamx.druid;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.initialization.ServerInit;
import com.metamx.druid.query.DefaultQueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.utils.PropUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.skife.config.ConfigurationObjectFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

/**
 */
public abstract class BaseServerNode<T extends QueryableNode> extends QueryableNode<T>
{
  private final Map<Class<? extends Query>, QueryRunnerFactory> additionalFactories = Maps.newLinkedHashMap();
  private QueryRunnerFactoryConglomerate conglomerate = null;
  private StupidPool<ByteBuffer> computeScratchPool = null;

  public BaseServerNode(
      Logger log,
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(log, props, lifecycle, jsonMapper, smileMapper, configFactory);
  }

  public QueryRunnerFactoryConglomerate getConglomerate()
  {
    initializeQueryRunnerFactoryConglomerate();
    return conglomerate;
  }

  public StupidPool<ByteBuffer> getComputeScratchPool()
  {
    initializeComputeScratchPool();
    return computeScratchPool;
  }

  @SuppressWarnings("unchecked")
  public T setConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    checkFieldNotSetAndSet("conglomerate", conglomerate);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setComputeScratchPool(StupidPool<ByteBuffer> computeScratchPool)
  {
    checkFieldNotSetAndSet("computeScratchPool", computeScratchPool);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T registerQueryRunnerFactory(Class<? extends Query> queryClazz, QueryRunnerFactory factory)
  {
    Preconditions.checkState(
        conglomerate == null,
        "Registering a QueryRunnerFactory only works when a separate conglomerate is not specified."
    );
    Preconditions.checkState(
        !additionalFactories.containsKey(queryClazz), "Registered factory for class[%s] multiple times", queryClazz
    );
    additionalFactories.put(queryClazz, factory);
    return (T) this;
  }

  private void initializeComputeScratchPool()
  {
    if (computeScratchPool == null) {
      setComputeScratchPool(
          ServerInit.makeComputeScratchPool(
              PropUtils.getPropertyAsInt(getProps(), "druid.computation.buffer.size", 1024 * 1024 * 1024)
          )
      );
    }
  }

  private void initializeQueryRunnerFactoryConglomerate()
  {
    if (conglomerate == null) {
      final Map<Class<? extends Query>, QueryRunnerFactory> factories = ServerInit.initDefaultQueryTypes(
          getConfigFactory(), getComputeScratchPool()
      );

      for (Map.Entry<Class<? extends Query>, QueryRunnerFactory> entry : additionalFactories.entrySet()) {
        factories.put(entry.getKey(), entry.getValue());
      }

      setConglomerate(new DefaultQueryRunnerFactoryConglomerate(factories));
    }
  }
}
