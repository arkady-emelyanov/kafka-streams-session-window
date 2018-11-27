package com.example.streams.session;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


/**
 * Streams topology testing
 */
public class AppTest {
    private TopologyTestDriver testDriver;
    private final String srcTopic = "src-events";
    private final String dstTopic = "dst-events";
    private final long slaWindowMs = 200;

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    @Before
    public void before() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        Topology topology = App.createTopology(srcTopic, dstTopic, slaWindowMs);
        testDriver = new TopologyTestDriver(topology, config, 0L);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    /**
     * Perform low-level topology driver testing
     * Expected all events, including tombstone.
     */
    @Test
    public void testApp() {
        ConsumerRecordFactory<String, String> factory =
                new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

        testDriver.pipeInput(factory.create(srcTopic, "1", "1", 1L));
        testDriver.pipeInput(factory.create(srcTopic, "2", "20", 20L));
        testDriver.pipeInput(factory.create(srcTopic, "1", "120", 120L));
        testDriver.pipeInput(factory.create(srcTopic, "2", "300", 300L));
        testDriver.advanceWallClockTime(slaWindowMs * 2);

        List<ProducerRecord<String, String>> list = new ArrayList<>();
        while (true) {
            ProducerRecord<String, String> record =
                    testDriver.readOutput(dstTopic, new StringDeserializer(), new StringDeserializer());
            if (record == null) {
                break;
            }

            list.add(record);
        }

        assertEquals(5, list.size());

        // first event
        assertEquals("1", list.get(0).key());
        assertEquals("1", list.get(0).value());
        assertEquals(1L, list.get(0).timestamp().longValue());

        // second event
        assertEquals("2", list.get(1).key());
        assertEquals("1", list.get(1).value());
        assertEquals(20L, list.get(1).timestamp().longValue());

        // expected tombstone event for key "1"
        assertEquals("1", list.get(2).key()); // tombstone record key
        assertNull(null, list.get(2).value()); // tombstone record value
        assertEquals(120L, list.get(2).timestamp().longValue()); // tombstone timestamp

        // corrected fist event
        assertEquals("1", list.get(3).key());
        assertEquals("2", list.get(3).value());
        assertEquals(120L, list.get(3).timestamp().longValue());

        // next window event (event arrived after defined window)
        assertEquals("2", list.get(4).key());
        assertEquals("1", list.get(4).value());
        assertEquals(300L, list.get(4).timestamp().longValue());
    }
}
