/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.guice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.druid.segment.realtime.FireDepartment;

import java.util.List;

/**
 */
public class FireDepartmentsProvider implements Provider<List<FireDepartment>>
{
  private final List<FireDepartment> fireDepartments = Lists.newArrayList();

  @Inject
  public FireDepartmentsProvider(
      ObjectMapper jsonMapper,
      RealtimeManagerConfig config
  )
  {
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
      throw Throwables.propagate(e);
    }
  }


  @Override
  public List<FireDepartment> get()
  {
    return fireDepartments;
  }
}
