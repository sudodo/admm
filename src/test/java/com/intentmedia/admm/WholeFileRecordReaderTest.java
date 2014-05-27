package com.intentmedia.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import java.io.*;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;

public class WholeFileRecordReaderTest extends EasyMockSupport {

    @Test
    public void testReadsGzipFile() throws Exception {
        // initialize inputs
        String fileContents = "lineOne\nlineTwo\n";
        LongWritable key = new LongWritable(0);
        Text value = new Text();

        // initialize mocks
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");

        File tempFile = File.createTempFile("/tmp", ".gz");
        Writer output = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tempFile)));
        output.write(fileContents);
        output.close();
        tempFile.deleteOnExit();

        FileSplit split = new FileSplit(new Path(tempFile.getAbsoluteFile().toURI().toString()),
                0L, tempFile.length(), new String[]{});

        replayAll();

        // initialize subject
        WholeFileRecordReader subject = new WholeFileRecordReader(conf, split);

        // invoke target
        subject.next(key, value);

        // assert
        assertEquals(value.toString(), fileContents);

        // verify
        verifyAll();
    }

}
