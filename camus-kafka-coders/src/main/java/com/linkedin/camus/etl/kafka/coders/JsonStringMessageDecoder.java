package com.linkedin.camus.etl.kafka.coders;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


/**
 * MessageDecoder class that will convert the payload into a JSON object,
 * look for a the camus.message.timestamp.field, convert that timestamp to
 * a unix epoch long using camus.message.timestamp.format, and then set the CamusWrapper's
 * timestamp property to the record's timestamp.  If the JSON does not have
 * a timestamp or if the timestamp could not be parsed properly, then
 * System.currentTimeMillis() will be used.
 * <p/>
 * camus.message.timestamp.format will be used with SimpleDateFormat.  If your
 * camus.message.timestamp.field is stored in JSON as a unix epoch timestamp,
 * you should set camus.message.timestamp.format to 'unix_seconds' (if your
 * timestamp units are seconds) or 'unix_milliseconds' (if your timestamp units
 * are milliseconds).
 * <p/>
 * This Decoder accepts a comma delimited list of possible timestamp fields in
 * camus.message.timestamp.field.  For each record, the first existent value
 * of one of these fields will be used as the camus timestamp for partitioning.
 * <p/>
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */
public class JsonStringMessageDecoder extends MessageDecoder<Message, String> {
  private static final org.apache.log4j.Logger log = Logger.getLogger(JsonStringMessageDecoder.class);

  // Property for format of timestamp in JSON timestamp field.
  public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
  public static final String DEFAULT_TIMESTAMP_FORMAT = "[dd/MMM/yyyy:HH:mm:ss Z]";

  // Property for the JSON field name of the timestamp.
  public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
  public static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";

  JsonParser jsonParser = new JsonParser();
  DateTimeFormatter dateTimeParser = ISODateTimeFormat.dateTimeParser();

  private String timestampFormat;
  private String timestampFieldProperty;
  private String[] timestampFields;

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;

    timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);

    // Kept only for logging purposes, the code uses timestampFields.
    timestampFieldProperty = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
    timestampFields = timestampFieldProperty.split(",");
  }

  @Override
  public CamusWrapper<String> decode(Message message) {
    long timestamp = 0;
    String payloadString;
    JsonObject jsonObject;

    try {
      payloadString = new String(message.getPayload(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Unable to load UTF-8 encoding, falling back to system default", e);
      payloadString = new String(message.getPayload());
    }

    // Parse the payload into a JsonObject.
    try {
      jsonObject = jsonParser.parse(payloadString.trim()).getAsJsonObject();
    } catch (RuntimeException e) {
      log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
      throw new RuntimeException(e);
    }

    // Find the timestampField value in the jsonObject
    JsonPrimitive timestampPrimitive = extractFirstJsonPrimitive(jsonObject, timestampFields);

    // Attempt to read and parse the timestamp element into a long.
    if (timestampPrimitive != null) {
      // If timestampFormat is 'unix_seconds',
      // then the timestamp only needs converted to milliseconds.
      // Also support 'unix' for backwards compatibility.
      if (timestampFormat.equals("unix_seconds") || timestampFormat.equals("unix")) {
        timestamp = timestampPrimitive.getAsLong();
        // This timestamp is in seconds, convert it to milliseconds.
        timestamp = timestamp * 1000L;
      }
      // Else if this timestamp is already in milliseconds,
      // just save it as is.
      else if (timestampFormat.equals("unix_milliseconds")) {
        timestamp = timestampPrimitive.getAsLong();
      }
      // Else if timestampFormat is 'ISO-8601', parse that
      else if (timestampFormat.equals("ISO-8601")) {
        String timestampString = timestampPrimitive.getAsString();
        try {
          timestamp = new DateTime(timestampString).getMillis();
        } catch (IllegalArgumentException e) {
          log.error("Could not parse timestamp '" + timestampString + "' as ISO-8601 while decoding JSON message.");
        }
      }
      // Otherwise parse the timestamp as a string in timestampFormat.
      else {
        String timestampString = timestampPrimitive.getAsString();
        try {
          timestamp = dateTimeParser.parseDateTime(timestampString).getMillis();
        } catch (IllegalArgumentException e) {
          try {
            timestamp = new SimpleDateFormat(timestampFormat).parse(timestampString).getTime();
          } catch (ParseException pe) {
            log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
          }
        } catch (Exception ee) {
          log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
        }
      }
    }

    // If timestamp wasn't set in the above block,
    // then set it to current time.
    if (timestamp == 0) {
      log.warn("Couldn't find or parse any timestamp fields '" + timestampFieldProperty
        + "' in JSON message, defaulting to current time.");
      timestamp = System.currentTimeMillis();
    }

    return new CamusWrapper<String>(payloadString, timestamp);
  }

  /**
   * Searches jsonObject for the specified jsonPath.  If jsonPath contains dots ('.'),
   * This will iterate through the json hierarchy to find the desired field.
   *
   * Example:
   * Given JsonObject jsonObject from {"k1": 1, "obj": {"k2": 2}}
   *   extractJsonPrimitive(jsonObject, "k1")     -> JsonPrimitive(1)
   *   extractJsonPrimitive(jsonObject, "obj.k2") -> JsonPrimitive(2)
   */
  private static JsonPrimitive extractJsonPrimitive(JsonObject jsonObject, String jsonPath) {
    // if the path has dots, assume the first element in the path is
    // in jsonObject, and the following elements are in subobjects.
    String[] paths = jsonPath.split("\\.", 2);
    if (jsonObject.has(paths[0])) {
      if (paths.length > 1) {
        // Recurse into the top level object to get the sub paths
        return extractJsonPrimitive(jsonObject.getAsJsonObject(paths[0]), paths[1]);
      }
      else {
        return jsonObject.getAsJsonPrimitive(paths[0]);
      }
    } else {
      return null;
    }
  }

  /**
   * Looks for values for each of the jsonPaths and returns the first non null result.
   * If no result it found, this returns null.
   * @param jsonObject
   * @param jsonPaths
   * @return
   */
  private static JsonPrimitive extractFirstJsonPrimitive(JsonObject jsonObject, String[] jsonPaths) {
    JsonPrimitive result = null;

    for (String jsonPath : jsonPaths) {
      result = extractJsonPrimitive(jsonObject, jsonPath);
      if (result != null) {
        break;
      }
    }

    return result;
  }
}
