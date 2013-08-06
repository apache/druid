package com.metamx.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Provider;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LocalCacheProvider.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "local", value = LocalCacheProvider.class),
    @JsonSubTypes.Type(name = "memcached", value = MemcachedCacheProvider.class)
})
public interface CacheProvider extends Provider<Cache>
{
}
