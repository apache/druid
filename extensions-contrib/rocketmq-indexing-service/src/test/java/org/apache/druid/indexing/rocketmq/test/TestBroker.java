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

package org.apache.druid.indexing.rocketmq.test;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class TestBroker implements Closeable
{
  private final File directory;
  private final File consumequeuedDirectory;
  private final File commitlogDirectory;
  private final boolean directoryCleanup;

  private volatile NamesrvController nameServer;
  private volatile BrokerController brokerServer;

  public TestBroker(
      @Nullable File directory
  )
  {
    this.directory = directory == null ? FileUtils.createTempDir() : directory;
    this.consumequeuedDirectory = new File(directory + "/consumequeue");
    this.commitlogDirectory = new File(directory + "/commitlog");
    this.directoryCleanup = directory == null;
  }

  public void start() throws Exception
  {
    startNameServer();
    startBroker();
  }

  public void startNameServer() throws Exception
  {
    NamesrvConfig namesrvConfig = new NamesrvConfig();
    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    nettyServerConfig.setListenPort(ThreadLocalRandom.current().nextInt(9999) + 10000);
    this.nameServer = new NamesrvController(namesrvConfig, nettyServerConfig);
    nameServer.initialize();
    nameServer.start();
  }

  public void startBroker() throws Exception
  {
    System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    directory.mkdir();
    consumequeuedDirectory.mkdir();
    commitlogDirectory.mkdir();

    BrokerConfig brokerConfig = new BrokerConfig();
    brokerConfig.setBrokerClusterName("test-cluster");
    brokerConfig.setBrokerName("broker-a");
    brokerConfig.setClusterTopicEnable(true);
    brokerConfig.setBrokerId(0);
    brokerConfig.setNamesrvAddr(StringUtils.format("127.0.0.1:%d", getPort()));

    NettyServerConfig brokerNettyServerConfig = new NettyServerConfig();
    NettyClientConfig brokerNettyClientConfig = new NettyClientConfig();
    brokerNettyServerConfig.setListenPort(ThreadLocalRandom.current().nextInt(9999) + 10000);
    MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
    messageStoreConfig.setBrokerRole("ASYNC_MASTER");
    messageStoreConfig.setFlushDiskType("ASYNC_FLUSH");
    messageStoreConfig.setStorePathRootDir(directory.toString());
    messageStoreConfig.setStorePathCommitLog(commitlogDirectory.toString());
    messageStoreConfig.setHaListenPort(ThreadLocalRandom.current().nextInt(9999) + 10000);

    this.brokerServer = new BrokerController(brokerConfig, brokerNettyServerConfig, brokerNettyClientConfig, messageStoreConfig);

    brokerServer.initialize();
    brokerServer.start();

  }

  public int getPort()
  {
    return nameServer.getNettyServerConfig().getListenPort();
  }

  public DefaultMQProducer newProducer()
  {
    DefaultMQProducer producer = new
        DefaultMQProducer("test_producer");
    producer.setNamesrvAddr(StringUtils.format("127.0.0.1:%d", getPort()));
    producer.setSendMsgTimeout(10000);
    return producer;
  }

  public Map<String, Object> consumerProperties()
  {
    final Map<String, Object> props = new HashMap<>();
    props.put("nameserver.url", StringUtils.format("127.0.0.1:%d", getPort()));
    return props;
  }

  @Override
  public void close() throws IOException
  {
    if (brokerServer != null) {
      brokerServer.shutdown();
      nameServer.shutdown();
    }
    if (directoryCleanup) {
      FileUtils.deleteDirectory(directory);
    }
  }
}
