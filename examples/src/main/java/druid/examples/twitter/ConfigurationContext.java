package druid.examples.twitter;

/*
* Copyright 2007 Yusuke Yamamoto
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
* Static factory of Configuration. This class wraps ConfigurationFactory implementations.<br>
* By default, twitter4j.conf.PropertyConfigurationFactory will be used and can be changed with -Dtwitter4j.configurationFactory system property.
*
*/
public final class ConfigurationContext {
  public static final String DEFAULT_CONFIGURATION_FACTORY = "twitter4j.conf.PropertyConfigurationFactory";
  public static final String CONFIGURATION_IMPL = "twitter4j.configurationFactory";
  private static final ConfigurationFactory factory;

  static {
    String CONFIG_IMPL;
    try {
      CONFIG_IMPL = System.getProperty(CONFIGURATION_IMPL, DEFAULT_CONFIGURATION_FACTORY);
    } catch (SecurityException ignore) {
      // Unsigned applets are not allowed to access System properties
      CONFIG_IMPL = DEFAULT_CONFIGURATION_FACTORY;
    }

    try {
      factory = (ConfigurationFactory) Class.forName(CONFIG_IMPL).newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new AssertionError(cnfe);
    } catch (InstantiationException ie) {
      throw new AssertionError(ie);
    } catch (IllegalAccessException iae) {
      throw new AssertionError(iae);
    }
  }


  public static Configuration getInstance() {
    return factory.getInstance();
  }

  public static Configuration getInstance(String configTreePath) {
    return factory.getInstance(configTreePath);
  }
}
