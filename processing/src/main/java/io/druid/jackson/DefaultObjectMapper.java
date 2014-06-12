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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

/**
 */
public class DefaultObjectMapper extends ObjectMapper
{
  public DefaultObjectMapper()
  {
    this((JsonFactory)null);
  }

  public DefaultObjectMapper(DefaultObjectMapper mapper)
  {
    super(mapper);
  }

  public DefaultObjectMapper(JsonFactory factory)
  {
    super(factory);
    registerModule(new DruidDefaultSerializersModule());
    registerModule(new GuavaModule());
    registerModule(new QueryGranularityModule());
    registerModule(new AggregatorsModule());
    registerModule(new SegmentsModule());

    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    configure(MapperFeature.AUTO_DETECT_GETTERS, false);
//    configure(MapperFeature.AUTO_DETECT_CREATORS, false); https://github.com/FasterXML/jackson-databind/issues/170
    configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    configure(SerializationFeature.INDENT_OUTPUT, false);
  }

  @Override
  public ObjectMapper copy()
  {
    return new DefaultObjectMapper(this);
  }
}
