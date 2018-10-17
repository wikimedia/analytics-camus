package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.Clock;

import org.apache.log4j.Logger;

public class TestStringMessageDecoder {
  private static final org.apache.log4j.Logger log = Logger.getLogger(StringMessageDecoder.class);

  @Test
  public void testTimestamp() {
    byte[] bytePayload = "I am a simple string".getBytes();
    long beforeTimestamp = System.currentTimeMillis();

    // Override the system's clock to mock the System.currentTimeMillis() call.
    StringMessageDecoder testDecoder = new StringMessageDecoder();
    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    long actualTimestamp = actualResult.getTimestamp();
    log.error(beforeTimestamp);
    log.error(actualTimestamp);
    assertTrue(beforeTimestamp < actualTimestamp);
  }
}
