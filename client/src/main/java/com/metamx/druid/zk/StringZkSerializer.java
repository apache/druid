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
