package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Properties;


/**
 * The StringMessageDecoder is a very simple class that takes as
 * input any string, tries to convert it to UTF-8 and then it applies
 * the current system timestamp rather than applying any heuristic to
 * find it in the payload. This is particularly useful for use cases in
 * which the message is not structured and the timestamp is not really
 * needed while importing data (for example: backups).
 */
public class StringMessageDecoder extends MessageDecoder<Message, String> {
  private static final org.apache.log4j.Logger log = Logger.getLogger(StringMessageDecoder.class);

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<String> decode(Message message) {
    String payloadString;

    try {
      payloadString = new String(message.getPayload(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Unable to load UTF-8 encoding, falling back to system default", e);
      payloadString = new String(message.getPayload());
    }

    return new CamusWrapper<String>(payloadString, System.currentTimeMillis());
  }
}
