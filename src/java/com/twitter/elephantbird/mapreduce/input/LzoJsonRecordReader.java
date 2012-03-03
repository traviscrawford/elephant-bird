package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes each line from an LZO compressed text file into a JSON map.
 * Skips lines that are invalid JSON. Multi-line JSON records are
 * not supported at this time.
 */
public class LzoJsonRecordReader extends RecordReader<LongWritable, MapWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoJsonRecordReader.class);

  private final LzoLineRecordReader reader = new LzoLineRecordReader();
  private final LongWritable key = new LongWritable();
  private final MapWritable value = new MapWritable();
  private final JSONParser parser = new JSONParser();

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    reader.initialize(inputSplit, taskAttemptContext);
  }

  @Override
  public synchronized void close() throws IOException {
    reader.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public MapWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return reader.getProgress();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    while (reader.nextKeyValue()) {
      // TODO(travis): Add success/failure counters
      if (decodeLineToJson(parser, reader.getCurrentValue(), value)) {
        return true;
      }
    }
    return false;
  }

  public static boolean decodeLineToJson(JSONParser parser, Text line, MapWritable value) {
    try {
      JSONObject jsonObj = (JSONObject)parser.parse(line.toString());
      if (jsonObj != null) {
        for (Object key: jsonObj.keySet()) {
          Text mapKey = new Text(key.toString());
          Text mapValue = new Text();
          if (jsonObj.get(key) != null) {
            mapValue.set(jsonObj.get(key).toString());
          }

          value.put(mapKey, mapValue);
        }
      }
      else {
          // JSONParser#parse(String) may return a null reference, e.g. when
          // the input parameter is the string "null".  A single line with
          // "null" is not valid JSON though.
          LOG.warn("Could not json-decode string: " + line);
          return false;
      }
      return true;
    } catch (ParseException e) {
      LOG.warn("Could not json-decode string: " + line, e);
      return false;
    } catch (NumberFormatException e) {
      LOG.warn("Could not parse field into number: " + line, e);
      return false;
    }
  }
}
