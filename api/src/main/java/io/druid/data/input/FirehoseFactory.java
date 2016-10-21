/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.druid.data.input.impl.InputRowParser;
import io.druid.java.util.common.parsers.ParseException;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface FirehoseFactory<T extends InputRowParser>
{
  /**
   * Initialization method that connects up the fire hose.  If this method returns successfully it should be safe to
   * call hasMore() on the returned Firehose (which might subsequently block).
   * <p/>
   * If this method returns null, then any attempt to call hasMore(), nextRow(), commit() and close() on the return
   * value will throw a surprising NPE.   Throwing IOException on connection failure or runtime exception on
   * invalid configuration is preferred over returning null.
   */
  public Firehose connect(T parser) throws IOException, ParseException;

}
