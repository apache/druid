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

package com.metamx.druid.guice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.logger.Logger;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.realtime.FireDepartment;
import com.metamx.druid.realtime.RealtimeManager;

import java.util.List;

/**
 */
public class RealtimeManagerProvider implements Provider<RealtimeManager>
{
  private static final Logger log = new Logger(RealtimeManagerProvider.class);

  private final QueryRunnerFactoryConglomerate conglomerate;
  private final List<FireDepartment> fireDepartments = Lists.newArrayList();

  @Inject
  public RealtimeManagerProvider(
      ObjectMapper jsonMapper,
      RealtimeManagerConfig config,
      QueryRunnerFactoryConglomerate conglomerate
  )
  {
    this.conglomerate = conglomerate;
    try {
      this.fireDepartments.addAll(
          (List<FireDepartment>) jsonMapper.readValue(
              config.getSpecFile(), new TypeReference<List<FireDepartment>>()
          {
          }
          )
      );
    }
    catch (Exception e) {
      log.error(e, "Unable to read fireDepartments from config");
    }
  }


  @Override
  public RealtimeManager get()
  {
    return new RealtimeManager(fireDepartments, conglomerate);
  }
}
