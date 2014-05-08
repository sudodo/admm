package com.intentmedia.admm;

import org.junit.Test;

public class AdmmOptimizerDriverTest {

    @Test(expected = NullPointerException.class)
    public void testIgnoresExtraArguments() throws Exception {
        String[] args = {
                "-outputPath", "outputPath",
                "-iterationsMaximum", "0",
                "-stepOutputBaseUrl", "abc",
                "-signalPath", "signalPath",
                "-regularizationFactor", "0.000001f"
        };
        AdmmOptimizerDriver subject = new AdmmOptimizerDriver();

        subject.run(args);
    }

}
