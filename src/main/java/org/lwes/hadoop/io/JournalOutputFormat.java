/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lwes.hadoop.io;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.lwes.hadoop.EventWritable;

/**
 *
 * @author rcongiu
 */
public class JournalOutputFormat extends FileOutputFormat<LongWritable, EventWritable>
        implements HiveOutputFormat {

    JobConf conf;

    @Override
    public RecordWriter<LongWritable, EventWritable> getRecordWriter(FileSystem fs,
            JobConf job, String name, Progressable progress) throws IOException {
        Path outputPath = getWorkOutputPath(job);

        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }
        Path file = new Path(outputPath, name);
        CompressionCodec codec = null;
        if (getCompressOutput(job)) {
            Class<?> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
        }

        final FSDataOutputStream outFile = fs.create(file);
        final DatagramPacketOutputStream out;
        if (codec != null) {
            out = new DatagramPacketOutputStream(codec.createOutputStream(outFile));
        } else {
            out = new DatagramPacketOutputStream(outFile);
        }

        return new RecordWriter<LongWritable, EventWritable>() {

            @Override
            public void close(Reporter reporter) throws IOException {
                out.close();
            }

            @Override
            public void write(LongWritable key, EventWritable value)
                    throws IOException {
                out.write(value.getEvent().serialize());
            }
        };
    }
    

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf job,
            Path path, Class type, boolean isCompressed, Properties properties,
            Progressable p) throws IOException {
        FileSystem fs = path.getFileSystem(job);

        final DatagramPacketOutputStream outStream = new  DatagramPacketOutputStream(Utilities.createCompressedStream(job,
                fs.create(path), isCompressed));

        int rowSeparator = 0;
        String rowSeparatorString = properties.getProperty(
                Constants.LINE_DELIM, "\n");
        try {
            rowSeparator = Byte.parseByte(rowSeparatorString);
        } catch (NumberFormatException e) {
            rowSeparator = rowSeparatorString.charAt(0);
        }
        final int finalRowSeparator = rowSeparator;
        return new FileSinkOperator.RecordWriter() {

            @Override
            public void write(Writable r) throws IOException {
                if (r instanceof Text) {
                    Text tr = (Text) r;
                    outStream.write(tr.getBytes(), 0, tr.getLength());
                    outStream.write(finalRowSeparator);
                } else if (r instanceof EventWritable) {
                    EventWritable ew = (EventWritable) r;
                    outStream.writeEvent(ew.getEvent());
                } else {
                    // DynamicSerDe always writes out BytesWritable
                    BytesWritable bw = (BytesWritable) r;
                    outStream.write(bw.get(), 0, bw.getSize());
                    outStream.write('\n');
                }
            }

            @Override
            public void close(boolean abort) throws IOException {
                outStream.close();
            }
        };

    }
}
