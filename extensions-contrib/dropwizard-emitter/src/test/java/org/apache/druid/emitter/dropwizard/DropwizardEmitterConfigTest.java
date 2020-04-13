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

package org.apache.druid.emitter.dropwizard;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.emitter.dropwizard.reporters.DropwizardConsoleReporter;
import org.apache.druid.emitter.dropwizard.reporters.DropwizardJMXReporter;
import org.apache.druid.guice.JsonConfigTesterBase;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class DropwizardEmitterConfigTest extends JsonConfigTesterBase<DropwizardEmitterConfig>
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    testProperties.put(getPropertyKey("reporters"), "[{\"type\":\"jmx\", \"domain\" : \"mydomain\"}]");
    propertyValues.put(getPropertyKey("reporters"), "[DropwizardJMXReporter{domain='mydomain'}]");
    propertyValues.put(getPropertyKey("includeHost"), "true");
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        ObjectMapper.class,
        new DefaultObjectMapper()
    ));
  }

  @Test
  public void testSerDeserDropwizardEmitterConfig() throws IOException
  {
    DropwizardEmitterConfig dropwizardEmitterConfig = new DropwizardEmitterConfig(
        Lists.newArrayList(new DropwizardConsoleReporter(), new DropwizardJMXReporter()),
        "my-prefix",
        false,
        "my/config/path",
        null,
        400
    );
    String dropwizardEmitterConfigString = mapper.writeValueAsString(dropwizardEmitterConfig);
    DropwizardEmitterConfig dropwizardEmitterConfigExpected = mapper.readerFor(DropwizardEmitterConfig.class).readValue(
        dropwizardEmitterConfigString
    );
    Assert.assertEquals(dropwizardEmitterConfigExpected, dropwizardEmitterConfig);
  }

  @Test
  public void testSerde()
  {
    propertyValues.put(getPropertyKey("reporters"), "[{\"type\":\"jmx\"}]");
    propertyValues.put(getPropertyKey("prefix"), "test-prefix");
    propertyValues.put(getPropertyKey("includeHost"), "true");
    testProperties.putAll(propertyValues);
    configProvider.inject(testProperties, configurator);
    DropwizardEmitterConfig config = configProvider.get().get();
    Assert.assertTrue("IncludeHost", config.getIncludeHost());
    Assert.assertEquals("test-prefix", config.getPrefix());
    Assert.assertEquals(1, config.getReporters().size());
    Assert.assertTrue("jmx reporter", config.getReporters().get(0) instanceof DropwizardJMXReporter);
  }

}


