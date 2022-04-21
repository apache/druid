package org.apache.druid.indexing.pulsar;

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

;

public class PulsarTestBase extends MockedPulsarServiceBaseTest
{

  @Override
  protected void setup() throws Exception
  {
    super.internalSetup();
    super.init();
    admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
    admin.tenants().createTenant("my-property",
        new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
    admin.namespaces().createNamespace("my-property/my-ns");
  }

  @Override
  protected void cleanup() throws Exception
  {
    super.internalCleanup();
  }

  public void setupCluster() throws Exception
  {
    setup();
  }

  public void tearDown() throws Exception
  {
    cleanup();
  }

  public PulsarAdmin getAdmin()
  {
    return admin;
  }

  public void sendData(String topic) throws PulsarClientException, UnsupportedEncodingException
  {
    Producer<byte[]> producer = pulsarClient.newProducer()
        .topic(topic)
        .create();

    final int messages = 100;
    for (int i = 0; i < messages; i++) {
      producer.send(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
    }
  }
}