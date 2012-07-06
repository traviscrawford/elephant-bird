package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.LzoRawInputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

public class DeprecatedLzoRawInputFormat
    extends DeprecatedInputFormatWrapper<LongWritable, BytesWritable> {

  public DeprecatedLzoRawInputFormat() {
    super(new LzoRawInputFormat());
  }
}
