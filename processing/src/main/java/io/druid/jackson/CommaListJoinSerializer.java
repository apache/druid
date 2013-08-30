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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.List;

/**
 */
public class CommaListJoinSerializer extends StdScalarSerializer<List<String>>
{
  private static final Joiner joiner = Joiner.on(",");

  protected CommaListJoinSerializer()
  {
    super(List.class, true);
  }

  @Override
  public void serialize(List<String> value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonGenerationException
  {
    jgen.writeString(joiner.join(value));
  }
}
