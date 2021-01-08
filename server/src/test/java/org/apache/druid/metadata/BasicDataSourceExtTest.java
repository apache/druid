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

package org.apache.druid.metadata;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.assertj.core.util.Lists;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Driver;
import java.util.List;
import java.util.Properties;

public class BasicDataSourceExtTest
{
  @Test
  public void testCreateConnectionFactory() throws Exception
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      private final List<String> passwords = Lists.newArrayList("pwd1", "pwd2");

      @Override
      public String getUser()
      {
        return "testuser";
      }

      @Override
      public String getPassword()
      {
        return passwords.remove(0);
      }
    };

    BasicDataSourceExt basicDataSourceExt = new BasicDataSourceExt(connectorConfig);

    basicDataSourceExt.setConnectionProperties("p1=v1");
    basicDataSourceExt.addConnectionProperty("p2", "v2");

    Driver driver = EasyMock.mock(Driver.class);
    Capture<String> uriArg = Capture.newInstance();
    Capture<Properties> propsArg = Capture.newInstance();
    EasyMock.expect(driver.connect(EasyMock.capture(uriArg), EasyMock.capture(propsArg))).andReturn(null).times(2);
    EasyMock.replay(driver);

    basicDataSourceExt.setDriver(driver);

    ConnectionFactory connectionFactory = basicDataSourceExt.createConnectionFactory();

    Properties expectedProps = new Properties();
    expectedProps.put("p1", "v1");
    expectedProps.put("p2", "v2");
    expectedProps.put("user", connectorConfig.getUser());


    Assert.assertNull(connectionFactory.createConnection());
    Assert.assertEquals(connectorConfig.getConnectURI(), uriArg.getValue());

    expectedProps.put("password", "pwd1");
    Assert.assertEquals(expectedProps, propsArg.getValue());

    Assert.assertNull(connectionFactory.createConnection());
    Assert.assertEquals(connectorConfig.getConnectURI(), uriArg.getValue());

    expectedProps.put("password", "pwd2");
    Assert.assertEquals(expectedProps, propsArg.getValue());
  }

  @Test
  public void testConnectionPropertiesHanding()
  {
    BasicDataSourceExt basicDataSourceExt = new BasicDataSourceExt(EasyMock.mock(MetadataStorageConnectorConfig.class));
    Properties expectedProps = new Properties();

    basicDataSourceExt.setConnectionProperties("");
    Assert.assertEquals(expectedProps, basicDataSourceExt.getConnectionProperties());

    basicDataSourceExt.setConnectionProperties("p0;p1=v1;p2=v2;p3=v3");
    basicDataSourceExt.addConnectionProperty("p4", "v4");
    basicDataSourceExt.addConnectionProperty("p5", "v5");
    basicDataSourceExt.removeConnectionProperty("p2");
    basicDataSourceExt.removeConnectionProperty("p5");

    expectedProps.put("p0", "");
    expectedProps.put("p1", "v1");
    expectedProps.put("p3", "v3");
    expectedProps.put("p4", "v4");

    Assert.assertEquals(expectedProps, basicDataSourceExt.getConnectionProperties());


  }
}
