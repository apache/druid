package io.druid.emitter.dropwizard;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.emitter.dropwizard.eventconverters.SendAllDropwizardEventConverter;
import io.druid.emitter.dropwizard.eventconverters.WhiteListBasedConverter;
import io.druid.emitter.dropwizard.reporters.DropwizardConsoleReporter;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class DropwizardEmitterConfigTest {
    private ObjectMapper mapper = new DefaultObjectMapper();

    @Before
    public void setUp()
    {
        mapper.setInjectableValues(new InjectableValues.Std().addValue(
                ObjectMapper.class,
                new DefaultObjectMapper()
        ));
    }

    @Test
    public void testSerDeserDropwizardEmitterConfig() throws IOException
    {
        DropwizardEmitterConfig dropwizardEmitterConfig = new DropwizardEmitterConfig(new DropwizardConsoleReporter(),new SendAllDropwizardEventConverter(null,false,false),new HistogramMetricManager(),null);
        String dropwizardEmitterConfigString = mapper.writeValueAsString(dropwizardEmitterConfig);
        DropwizardEmitterConfig dropwizardEmitterConfigExpected = mapper.reader(DropwizardEmitterConfig.class).readValue(
                dropwizardEmitterConfigString
        );
        Assert.assertEquals(dropwizardEmitterConfigExpected, dropwizardEmitterConfig);
    }

    @Test
    public void testSerDeserDruidToDropwizardEventConverter() throws IOException
    {
        SendAllDropwizardEventConverter sendAllDropwizardEventConverter = new SendAllDropwizardEventConverter("prefix", true, true);
        String noopDropwizardEventConverterString = mapper.writeValueAsString(sendAllDropwizardEventConverter);
        DruidToDropwizardEventConverter druidToDropwizardEventConverter = mapper.reader(DruidToDropwizardEventConverter.class)
                .readValue(noopDropwizardEventConverterString);
        Assert.assertEquals(druidToDropwizardEventConverter, sendAllDropwizardEventConverter);

        WhiteListBasedConverter whiteListBasedConverter = new WhiteListBasedConverter(
                "prefix",
                true,
                true,
                "",
                new DefaultObjectMapper()
        );
        String whiteListBasedConverterString = mapper.writeValueAsString(whiteListBasedConverter);
        druidToDropwizardEventConverter = mapper.reader(DruidToDropwizardEventConverter.class)
                .readValue(whiteListBasedConverterString);
        Assert.assertEquals(druidToDropwizardEventConverter, whiteListBasedConverter);
    }

    }


