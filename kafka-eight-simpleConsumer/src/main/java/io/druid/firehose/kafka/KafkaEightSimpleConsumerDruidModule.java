package io.druid.firehose.kafka;

import java.util.List;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;

import io.druid.initialization.DruidModule;

public class KafkaEightSimpleConsumerDruidModule implements DruidModule{
	 @Override
	  public List<? extends Module> getJacksonModules()
	  {
	    return ImmutableList.of(
	        new SimpleModule("KafkaEightSimpleConsumerFirehoseModule")
	            .registerSubtypes(
	                new NamedType(KafkaEightSimpleConsumerFirehoseFactory.class, "kafka-0.8-simpaleConsumer")
	            )
	    );
	  }

	  @Override
	  public void configure(Binder binder)
	  {

	  }
}
