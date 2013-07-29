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

package com.metamx.druid.indexer.data;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Charsets;
import com.metamx.common.exception.FormattedException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ToLowerCaseParser;
import com.metamx.druid.input.InputRow;

/**
 */
public class StringInputRowParser implements ByteBufferInputRowParser
{
	private final InputRowParser<Map<String, Object>> inputRowCreator;
	private final Parser<String, Object> parser;

	private CharBuffer chars = null;

	@JsonCreator
	public StringInputRowParser(
	    @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
	    @JsonProperty("data") DataSpec dataSpec,
	    @JsonProperty("dimensionExclusions") List<String> dimensionExclusions)
	{
		this.inputRowCreator = new MapInputRowParser(timestampSpec, dataSpec.getDimensions(), dimensionExclusions);
		this.parser = new ToLowerCaseParser(dataSpec.getParser());
	}

	public void addDimensionExclusion(String dimension)
	{
		inputRowCreator.addDimensionExclusion(dimension);
	}

	@Override
	public InputRow parse(ByteBuffer input) throws FormattedException
	{
		return parseMap(buildStringKeyMap(input));
	}

	private Map<String, Object> buildStringKeyMap(ByteBuffer input)
	{
		int payloadSize = input.limit();

		if (chars == null || chars.remaining() < payloadSize)
		{
			chars = CharBuffer.allocate(payloadSize);
		}

		final CoderResult coderResult = Charsets.UTF_8.newDecoder()
		    .onMalformedInput(CodingErrorAction.REPLACE)
		    .onUnmappableCharacter(CodingErrorAction.REPLACE)
		    .decode(input, chars, true);

		Map<String, Object> theMap;
		if (coderResult.isUnderflow())
		{
			chars.flip();
			try
			{
				theMap = parseString(chars.toString());
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
		return theMap;
	}

	private Map<String, Object> parseString(String inputString)
	{
		return parser.parse(inputString);
	}

	public InputRow parse(String input) throws FormattedException
	{
		return parseMap(parseString(input));
	}

	private InputRow parseMap(Map<String, Object> theMap)
	{
		return inputRowCreator.parse(theMap);
	}

	@JsonValue
	public InputRowParser<Map<String, Object>> getInputRowCreator()
	{
		return inputRowCreator;
	}
}
