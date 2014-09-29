/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;

import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.segment.realtime.plumber.PlumberSchool;

/**
 */
public class RealtimeIOConfig implements IOConfig
{
  private final FirehoseFactory firehoseFactory;
  private final PlumberSchool plumberSchool;
  private final FirehoseFactoryV2 firehoseFactoryV2;
	private static final Logger log = new Logger(
			RealtimeIOConfig.class);
  @JsonCreator
  public RealtimeIOConfig(
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("plumber") PlumberSchool plumberSchool,
      @JsonProperty("firehoseV2") FirehoseFactoryV2 firehoseFactoryV2
  )
  {
    this.firehoseFactory = firehoseFactory;
    this.plumberSchool = plumberSchool;
    this.firehoseFactoryV2 = firehoseFactoryV2;
    try{
  		log.info("initializing realtime io config firehose [%s] - firehoseV2 [%s]", firehoseFactory, firehoseFactoryV2);
  	} catch (Exception e) {
  		log.info("failed to get initialization firehose");
  	}
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
  	log.info("get Firehose");
    return firehoseFactory;
  }
  
  @JsonProperty("firehoseV2")
  public FirehoseFactoryV2 getFirehoseFactoryV2()
  {
  	try{
  		log.info("initializing realtime io config firehose [%s] - firehoseV2 [%s]", firehoseFactory, firehoseFactoryV2);
  	} catch (Exception e) {
  		log.info("failed to get initialization firehose");
  	}
  	return firehoseFactoryV2;
  }

  public PlumberSchool getPlumberSchool()
  {
    return plumberSchool;
  }
}
