package com.intentmedia.admm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

import static org.apache.hadoop.mapred.LineRecordReader.LineReader;

/**
 * Treats keys as offset in file and value as line.
 */
public class WholeFileRecordReader implements RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(WholeFileRecordReader.class.getName());
    private static final int INT_SIZE_IN_BYTES = 4;
    private static final int B1_OFFSET = 24;
    private static final int B2_OFFSET = 16;
    private static final int B3_OFFSET = 8;
    private static final int BITWISE_AND_VALUE = 0xff;
    private static final int MAX_READ_TRIES = 3;
    private CompressionCodecFactory compressionCodecs = null;
    private final int maxLineLength;
    private final CompressionCodec codec;
    private final Configuration job;
    private final FileSplit split;
    private final Path file;

    private long pos;
    private long start;
    private long end;
    private LineReader in;

    public WholeFileRecordReader(Configuration job, FileSplit split) throws IOException {
        this.job = job;
        this.split = split;
        this.file = split.getPath();

        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        codec = compressionCodecs.getCodec(file);

        resetReading();
    }

    private int getUncompressedFileLength(FSDataInputStream fileIn, long uncompressedSize) throws IOException {
        fileIn.seek(uncompressedSize - INT_SIZE_IN_BYTES);
        byte b4 = fileIn.readByte();
        byte b3 = fileIn.readByte();
        byte b2 = fileIn.readByte();
        byte b1 = fileIn.readByte();

        return ((b1 & BITWISE_AND_VALUE) << B1_OFFSET) | ((b2 & BITWISE_AND_VALUE) << B2_OFFSET) | ((b3 & BITWISE_AND_VALUE) << B3_OFFSET) | (b4 & BITWISE_AND_VALUE);
    }

    private void resetReading() throws IOException {
        long startInit = split.getStart();
        long endInit = startInit + split.getLength();

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            endInit = startInit + getUncompressedFileLength(fileIn, split.getLength());
            fileIn.seek(0);
            in = new LineReader(codec.createInputStream(fileIn), job);
        } else {
            if (startInit != 0) {
                skipFirstLine = true;
                --startInit;
                fileIn.seek(startInit);
            }
        }
        if (skipFirstLine) {
            // skip first line and re-establish "start".
            startInit += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, endInit - startInit));
        }
        this.start = startInit;
        this.end = endInit;
        this.pos = startInit;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    /**
     * Read a line.
     */
    @Override
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        int newSize = 0;
        int numberOfTries = 0;

        while (newSize == 0 && numberOfTries < MAX_READ_TRIES) {
            try {
                resetReading();

                numberOfTries++;
                key.set(pos);
                Text lineValue = new Text();
                newSize = 0;
                StringBuilder resultBuffer = new StringBuilder();
                while (pos < end) {
                    int lineSize = in.readLine(lineValue,
                            maxLineLength,
                            Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
                    if (lineSize == 0) {
                        return newSize > 0;
                    }
                    pos += lineSize;
                    newSize += lineSize;
                    resultBuffer.append(lineValue.toString()).append("\n");
                    if (lineSize > maxLineLength) {
                        // line too long. try again
                        LOG.info(String.format("Skipped line of size %d at pos %d", lineSize, (pos - lineSize)));
                    }
                }
                value.set(resultBuffer.toString());
                lineValue.clear();
            }
            catch (IOException e) {
                LOG.info(String.format("Failed to read file with key %d in %d tries", key.get(), numberOfTries));
                if (numberOfTries == MAX_READ_TRIES) {
                    throw new IOException(e);
                }
            }
        }
        LOG.info(String.format("Success, read file with key %d in %d tries", key.get(), numberOfTries));
        return newSize > 0;
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