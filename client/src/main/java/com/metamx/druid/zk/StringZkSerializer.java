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

package com.metamx.druid.zk;

import com.metamx.common.IAE;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.nio.charset.Charset;

/**
*/
public class StringZkSerializer implements ZkSerializer
{
  private static final Charset UTF8 = Charset.forName("UTF8");

  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError
  {
    if (data instanceof String) {
      return ((String) data).getBytes(UTF8);
    }
    throw new IAE("Can only serialize strings into ZK");
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError
  {
    return new String(bytes, UTF8);
  }
}
