package com.metamx.druid.curator.discovery;

import com.google.common.base.Throwables;
import org.apache.curator.x.discovery.ServiceInstance;

public class AddressPortServiceInstanceFactory implements ServiceInstanceFactory<Void>
{
  private final String address;
  private final int port;

  public AddressPortServiceInstanceFactory(String address, int port)
  {
    this.address = address;
    this.port = port;
  }

  @Override
  public ServiceInstance<Void> create(String service)
  {
    try {
      return ServiceInstance.<Void>builder().name(service).address(address).port(port).build();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
