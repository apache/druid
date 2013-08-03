package com.metamx.druid.client;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Provider;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = SingleServerInventoryProvider.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "legacy", value = SingleServerInventoryProvider.class),
    @JsonSubTypes.Type(name = "batch", value = BatchServerInventoryViewProvider.class)
})
public interface ServerInventoryViewProvider extends Provider<ServerInventoryView>
{
}
