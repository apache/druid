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

package io.druid;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.ISE;
import io.druid.guice.ServerModule;
import io.druid.jackson.DefaultObjectMapper;

import java.util.List;

/**
 */
public class TestUtil
{
  public static final ObjectMapper MAPPER = new DefaultObjectMapper();

  static {
    final List<? extends Module> list = new ServerModule().getJacksonModules();
    for (Module module : list) {
      MAPPER.registerModule(module);
    }
    MAPPER.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            if (valueId.equals("com.fasterxml.jackson.databind.ObjectMapper")) {
              return TestUtil.MAPPER;
            }
            throw new ISE("No Injectable value found");
          }
        }
    );
  }
}
