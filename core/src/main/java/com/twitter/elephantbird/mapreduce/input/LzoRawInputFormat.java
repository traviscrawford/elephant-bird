package com.twitter.elephantbird.mapreduce.input;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LzoRawInputFormat extends LzoInputFormat<LongWritable, BytesWritable> {
  @Override
  public RecordReader<LongWritable, BytesWritable> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    // TODO: Handle block format.
    return new LzoRawB64LineRecordReader();
  }
}
