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

package io.druid.data.input.thrift.util;

import io.druid.data.input.test.TestThriftObj;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ThriftUtilsTest
{

  @Test
  public void convert_one_thrift_obj_base64()
  {
    TestThriftObj thriftObj1 = new TestThriftObj(), thriftObj2;
    thriftObj1.setId(1);
    thriftObj1.setName("name1");
    thriftObj1.setTimestamp(1234L);
    thriftObj1.addToBlackList("invalidip");

    String encodedStr = ThriftUtils.encodeBase64String(thriftObj1);
    thriftObj2 = ThriftUtils.decodeBase64String(encodedStr, new TestThriftObj());
    assertEquals(thriftObj1, thriftObj2);
  }


  @Test
  public void convert_thrift_obj_base64_for_reuse()
  {
    TestThriftObj thriftObj1 = new TestThriftObj(), thriftObj2 = new TestThriftObj();
    TestThriftObj outThriftObj = new TestThriftObj();

    thriftObj1.setId(1);
    thriftObj1.setName("name2");
    thriftObj1.setTimestamp(2345L);

    thriftObj2.setId(2);
    thriftObj2.setTimestamp(3456L);
    thriftObj2.addToBlackList("myiplist");

    // Test re-using the output thrift object
    String encodedStr1 = ThriftUtils.encodeBase64String(thriftObj1);
    outThriftObj = ThriftUtils.decodeBase64String(encodedStr1, thriftObj2);
    assertSame(thriftObj2, outThriftObj);
    assertEquals(thriftObj1, outThriftObj);

    String encodedStr2 = ThriftUtils.encodeBase64String(thriftObj2);
    outThriftObj = ThriftUtils.decodeBase64String(encodedStr2, thriftObj2);
    assertSame(thriftObj2, outThriftObj);
    assertEquals(thriftObj2, outThriftObj);

    String encodedStr3 = ThriftUtils.encodeBase64String(thriftObj1);
    outThriftObj = ThriftUtils.decodeBase64String(encodedStr3, thriftObj2);
    assertSame(thriftObj2, outThriftObj);
    assertEquals(thriftObj1, outThriftObj);
  }


}
