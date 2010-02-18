/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lwes.hadoop.io;



import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.lwes.Event;
import org.lwes.hadoop.EventWritable;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class JournalInputFormat extends FileInputFormat<LongWritable,EventWritable>
  implements JobConfigurable {

  JobConf conf;

  @Override
  public void configure(JobConf conf) {
    this.conf = conf;
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path file) {
      return false;
  }

  @Override
  public RecordReader<LongWritable,EventWritable> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
    throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new JournalRecordReader(job, (FileSplit) genericSplit);
  }
  /*************************************************************
   * RecordReader
   *
   *************************************************************/
  public static class JournalRecordReader implements RecordReader<LongWritable,EventWritable> {

    private static final Log LOG = LogFactory.getLog(JournalRecordReader.class.getName());
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    int maxLineLength;
    private final DatagramPacketInputStream in;

    public JournalRecordReader(Configuration job,
            FileSplit split) throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        LOG.debug("RecordReader: processing path " + file.toString() );

        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());

        if (codec != null) {
            in = new DatagramPacketInputStream(codec.createInputStream(fileIn));
        } else {
            in =  new DatagramPacketInputStream(fileIn);
        }
    }


    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public EventWritable createValue() {
        return new EventWritable();
    }

    /** Read a line. */
    @Override
    public synchronized boolean next(LongWritable key, EventWritable value)
            throws IOException {

        Event ev = in.readEvent();

        if (ev != null) {
            value.setEvent(ev);
            pos += ev.serialize().length;

            key.set(pos);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Get the progress within the split
     */
    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}


}
