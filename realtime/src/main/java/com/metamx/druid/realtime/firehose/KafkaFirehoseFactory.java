/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.realtime.firehose;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.exception.FormattedException;
import com.metamx.common.logger.Logger;
import com.metamx.druid.indexer.data.InputRowParser;
import com.metamx.druid.indexer.data.ProtoBufInputRowParser;
import com.metamx.druid.indexer.data.StringInputRowParser;
import com.metamx.druid.input.InputRow;

/**
 */
public class KafkaFirehoseFactory implements FirehoseFactory
{
	private static final Logger log = new Logger(KafkaFirehoseFactory.class);

	@JsonProperty
	private final Properties consumerProps;

	@JsonProperty
	private final String feed;

	@JsonProperty
	private final InputRowParser parser;

	@JsonCreator
	public KafkaFirehoseFactory(
	    @JsonProperty("consumerProps") Properties consumerProps,
	    @JsonProperty("feed") String feed,
	    @JsonProperty("parser") InputRowParser parser)
	{
		this.consumerProps = consumerProps;
		this.feed = feed;
		this.parser = parser;

		parser.addDimensionExclusion("feed");
	}

	@Override
	public Firehose connect() throws IOException
	{
		final ConsumerConnector connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));

		final Map<String, List<KafkaStream<Message>>> streams = connector.createMessageStreams(ImmutableMap.of(feed, 1));

		final List<KafkaStream<Message>> streamList = streams.get(feed);
		if (streamList == null || streamList.size() != 1)
		{
			return null;
		}

		final KafkaStream<Message> stream = streamList.get(0);

		if (parser instanceof StringInputRowParser)
		{

			return new StringMessageFirehose(connector, stream);
		} else if (parser instanceof ProtoBufInputRowParser)
		{
			return new ProtoBufMessageFirehose(stream, connector);
		}
		throw new RuntimeException("No Firehose for parser: " + parser.getClass().getName());
	}

	private abstract static class AbstractKafkaFirehose implements Firehose
	{
		protected final ConsumerConnector connector;
		protected final Iterator<MessageAndMetadata<Message>> iter;

		public AbstractKafkaFirehose(ConsumerConnector connector, KafkaStream<Message> stream)
		{
			iter = stream.iterator();
			this.connector = connector;
		}

		@Override
		public boolean hasMore()
		{
			return iter.hasNext();
		}

		@Override
		public InputRow nextRow() throws FormattedException
		{
			final Message message = iter.next().message();

			if (message == null)
			{
				return null;
			}

			return parseMessage(message);
		}

		protected abstract InputRow parseMessage(Message message);

		@Override
		public Runnable commit()
		{
			return new Runnable()
			{
				@Override
				public void run()
				{
					/*
					 * This is actually not going to do exactly what we want, cause it
					 * will be called asynchronously after the persist is complete. So,
					 * it's going to commit that it's processed more than was actually
					 * persisted. This is unfortunate, but good enough for now. Should
					 * revisit along with an upgrade of our Kafka version.
					 */

					log.info("committing offsets");
					connector.commitOffsets();
				}
			};
		}

		@Override
		public void close() throws IOException
		{
			connector.shutdown();
		}
	}

	private class StringMessageFirehose extends AbstractKafkaFirehose
	{

		private CharBuffer chars = null;

		public StringMessageFirehose(ConsumerConnector connector, KafkaStream<Message> stream)
		{
			super(connector, stream);
		}

		@Override
		public InputRow parseMessage(Message message) throws FormattedException
		{

			int payloadSize = message.payloadSize();
			if (chars == null || chars.remaining() < payloadSize)
			{
				chars = CharBuffer.allocate(payloadSize);
			}

			final CoderResult coderResult = Charsets.UTF_8.newDecoder()
			    .onMalformedInput(CodingErrorAction.REPLACE)
			    .onUnmappableCharacter(CodingErrorAction.REPLACE)
			    .decode(message.payload(), chars, true);

			if (coderResult.isUnderflow())
			{
				chars.flip();
				try
				{
					return parser.parse(chars.toString());
				} finally
				{
					chars.clear();
				}
			}
			else
			{
				throw new FormattedException.Builder()
				    .withErrorCode(FormattedException.ErrorCode.UNPARSABLE_ROW)
				    .withMessage(String.format("Failed with CoderResult[%s]", coderResult))
				    .build();
			}
		}

	}

	private class ProtoBufMessageFirehose extends AbstractKafkaFirehose
	{

		private ByteBuffer bytes = null;

		public ProtoBufMessageFirehose(KafkaStream<Message> stream, ConsumerConnector connector)
		{
			super(connector, stream);
		}

		@Override
		public InputRow parseMessage(Message message) throws FormattedException
		{

			int payloadSize = message.payloadSize();
			if (bytes == null || bytes.remaining() < payloadSize)
			{
				bytes = ByteBuffer.allocate(payloadSize);
			}

			bytes.put(message.payload());

			bytes.flip();
			try
			{
				return parser.parse(bytes);
			} finally
			{
				bytes.clear();
			}
		}

	}
}
