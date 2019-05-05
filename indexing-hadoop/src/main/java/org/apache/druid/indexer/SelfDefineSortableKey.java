/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Modified from the SelfDefineSortableKey in https://github.com/apache/kylin
 */
public class SelfDefineSortableKey implements WritableComparable<SelfDefineSortableKey>
{

  public enum TypeFlag
  {
    NONE_NUMERIC_TYPE,
    INTEGER_FAMILY_TYPE,
    DOUBLE_FAMILY_TYPE
  }

  private byte typeId = (byte) TypeFlag.NONE_NUMERIC_TYPE.ordinal(); //non-numeric(0000 0000) int(0000 0001) other numberic(0000 0010)

  private Text rawKey;

  private Object keyInObj;

  public SelfDefineSortableKey()
  {
  }

  public SelfDefineSortableKey(Text key)
  {
    init(key, (byte) TypeFlag.NONE_NUMERIC_TYPE.ordinal());
  }

  public void init(Text key, byte typeId)
  {
    this.typeId = typeId;
    this.rawKey = key;
    this.keyInObj = key;
  }

  public void init(Text key)
  {
    init(key, (byte) TypeFlag.NONE_NUMERIC_TYPE.ordinal());
  }

  @Override
  public int compareTo(SelfDefineSortableKey o)
  {
    if (this.typeId != o.typeId) {
      throw new IllegalStateException("Error. Incompatible types");
    }

    return ((Text) this.keyInObj).compareTo(((Text) o.keyInObj));
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException
  {
    dataOutput.writeByte(typeId);
    rawKey.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException
  {
    this.typeId = dataInput.readByte();
    Text inputKey = new Text();
    inputKey.readFields(dataInput);
    init(inputKey, typeId);
  }

  public short getTypeId()
  {
    return typeId;
  }

  public Text getText()
  {
    return rawKey;
  }

  public void setTypeId(byte typeId)
  {
    this.typeId = typeId;
  }
}


