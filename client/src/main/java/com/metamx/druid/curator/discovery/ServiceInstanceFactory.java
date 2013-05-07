package com.metamx.druid.curator.discovery;

import org.apache.curator.x.discovery.ServiceInstance;

public interface ServiceInstanceFactory<T>
{
  public ServiceInstance<T> create(String service);
}
