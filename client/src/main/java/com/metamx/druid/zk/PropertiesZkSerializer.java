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

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

/**
*/
public class PropertiesZkSerializer implements ZkSerializer
{
  private static final SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss'z'");
  static {
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
  public final static String META_PROP = "__MODIFIED";

  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError
  {
    if (data instanceof Properties) {
      final Properties props = (Properties) data;
      ByteArrayOutputStream bos = new ByteArrayOutputStream(props.size()*60 + 30);
      try {
        final String ts = df.format(new Date());
        props.setProperty("__MODIFIED", ts);
        props.store(bos, "Druid");
      } catch (IOException ignored) { }
      return bos.toByteArray();
    }
    throw new IAE("Can only serialize Properties into ZK");
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError
  {
    final Properties props = new Properties();
    try {
      props.load(new ByteArrayInputStream(bytes));
    } catch (IOException ignored) {
    }
    return props;
  }
}
