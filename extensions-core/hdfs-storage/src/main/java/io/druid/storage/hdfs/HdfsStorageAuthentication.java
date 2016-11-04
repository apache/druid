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

package io.druid.storage.hdfs;


import com.google.common.base.Strings;
import com.google.inject.Inject;

import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

@ManageLifecycle
public class HdfsStorageAuthentication
{
  private static final Logger log = new Logger(HdfsStorageAuthentication.class);
  private final HdfsKerberosConfig hdfsKerberosConfig;
  private final Configuration hadoopConf;

  @Inject
  public HdfsStorageAuthentication(HdfsKerberosConfig hdfsKerberosConfig, Configuration hadoopConf)
  {
    this.hdfsKerberosConfig = hdfsKerberosConfig;
    this.hadoopConf = hadoopConf;
  }

  /**
   * Dose authenticate against a secured hadoop cluster
   * In case of any bug fix make sure to fix the code in JobHelper#authenticate as well.
   */
  @LifecycleStart
  public void authenticate()
  {
    String principal = hdfsKerberosConfig.getPrincipal();
    String keytab = hdfsKerberosConfig.getKeytab();
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      UserGroupInformation.setConfiguration(hadoopConf);
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          if (UserGroupInformation.getCurrentUser().hasKerberosCredentials() == false
              || !UserGroupInformation.getCurrentUser().getUserName().equals(principal)) {
            log.info("Trying to authenticate user [%s] with keytab [%s]..", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
          }
        }
        catch (IOException e) {
          throw new ISE(e, "Failed to authenticate user principal [%s] with keytab [%s]", principal, keytab);
        }
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    //noop
  }
}
