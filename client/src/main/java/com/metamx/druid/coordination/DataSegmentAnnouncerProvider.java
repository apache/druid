package com.metamx.druid.coordination;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Provider;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyDataSegmentAnnouncerProvider.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "legacy", value = LegacyDataSegmentAnnouncerProvider.class),
    @JsonSubTypes.Type(name = "batch", value = BatchDataSegmentAnnouncerProvider.class)
})
public interface DataSegmentAnnouncerProvider extends Provider<DataSegmentAnnouncer>
{
}
