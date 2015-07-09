package io.druid.firehose.rocketmq;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.List;

public class RocketMQDruidModule implements DruidModule {

    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new SimpleModule("RocketMQFirehoseModule")
                        .registerSubtypes(
                                new NamedType(RocketMQFirehoseFactory.class, "RocketMQ-3.2.2.R2")
                        )
        );
    }

    @Override
    public void configure(Binder binder) {

    }
}
