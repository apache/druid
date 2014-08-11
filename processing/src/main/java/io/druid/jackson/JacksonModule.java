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

package io.druid.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;

/**
 */
public class JacksonModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(ObjectMapper.class).to(Key.get(ObjectMapper.class, Json.class));
  }

  @Provides @LazySingleton @Json
  public ObjectMapper jsonMapper()
  {
    return new DefaultObjectMapper();
  }

  @Provides @LazySingleton @Smile
  public ObjectMapper smileMapper()
  {
    ObjectMapper retVal = new DefaultObjectMapper(new SmileFactory());
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }
}
