package org.apache.druid.k8s.overlord.common;

import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

@FunctionalInterface
public interface KubernetesInformerExecutor<T, R>
{
  T executeRequest(SharedIndexInformer<R> informer);
}
