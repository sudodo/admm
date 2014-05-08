package com.intentmedia.admm;

import org.junit.Test;
import org.kohsuke.args4j.CmdLineParser;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class AdmmOptimizerDriverArgumentsTest {

    private static final URI OUTPUT_PATH = URI.create("s3n://my-bucket/output/XXX/");
    private static final String SIGNAL_PATH = "inputPath";

    @Test
    public void testSignalPath() throws Exception {
        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        new CmdLineParser(subject).parseArgument(
                "-outputPath", OUTPUT_PATH.toString(),
                "-signalPath", SIGNAL_PATH);

        assertEquals(SIGNAL_PATH, subject.getSignalPath());
    }

    @Test
    public void testIterationsMaximum() throws Exception {
        Integer iterationsMaximum = 1;

        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        new CmdLineParser(subject).parseArgument(
                "-outputPath", OUTPUT_PATH.toString(),
                "-signalPath", SIGNAL_PATH,
                "-iterationsMaximum", iterationsMaximum.toString());

        assertEquals((int) iterationsMaximum, subject.getIterationsMaximum());
    }

    @Test
    public void testRegularizationFactor() throws Exception {
        Double regularizationFactor = 0.5;

        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        new CmdLineParser(subject).parseArgument(
                "-outputPath", OUTPUT_PATH.toString(),
                "-signalPath", SIGNAL_PATH,
                "-regularizationFactor", regularizationFactor.toString());

        assertEquals((double) regularizationFactor, subject.getRegularizationFactor(), 0.0);
    }

    @Test
    public void testAddIntercept() throws Exception {
        boolean addIntercept = true;

        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        new CmdLineParser(subject).parseArgument(
                "-outputPath", OUTPUT_PATH.toString(),
                "-signalPath", SIGNAL_PATH,
                "-addIntercept");

        assertEquals(addIntercept, subject.getAddIntercept());
    }

    @Test
    public void testRegularizeIntercept() throws Exception {
        boolean regularizeIntercept = true;

        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        new CmdLineParser(subject).parseArgument(
                "-outputPath", OUTPUT_PATH.toString(),
                "-signalPath", SIGNAL_PATH,
                "-regularizeIntercept");

        assertEquals(regularizeIntercept, subject.getRegularizeIntercept());
    }

    @Test
    public void testColumnsToExclude() throws Exception {
        String columnsToExclude = "columnsToExclude";

        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        new CmdLineParser(subject).parseArgument(
                "-outputPath", OUTPUT_PATH.toString(),
                "-signalPath", SIGNAL_PATH,
                "-columnsToExclude", columnsToExclude);

        assertEquals(columnsToExclude, subject.getColumnsToExclude());
    }
}
