package com.intentmedia.admm;

import com.intentmedia.bfgs.optimize.BFGS;
import com.intentmedia.bfgs.optimize.IOptimizer;
import com.intentmedia.bfgs.optimize.LogisticL2DifferentiableFunction;
import com.intentmedia.bfgs.optimize.OptimizerParameters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.intentmedia.admm.AdmmIterationHelper.*;

public class AdmmIterationMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, Text> {

    private static final IntWritable ZERO = new IntWritable(0);
    private static final Logger LOG = Logger.getLogger(AdmmIterationMapper.class.getName());
    private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
    private static final float DEFAULT_RHO = 0.1f;

    private int iteration;
    private FileSystem fs;
    private Map<String, String> splitToParameters;
    private Set<Integer> columnsToExclude;

    private OptimizerParameters optimizerParameters = new OptimizerParameters();
    private BFGS<LogisticL2DifferentiableFunction> bfgs = new BFGS<LogisticL2DifferentiableFunction>(optimizerParameters);
    private boolean addIntercept;
    private float regularizationFactor;
    private double rho;
    private String previousIntermediateOutputLocation;
    private Path previousIntermediateOutputLocationPath;

    @Override
    public void configure(JobConf job) {
        iteration = Integer.parseInt(job.get("iteration.number"));
        String columnsToExcludeString = job.get("columns.to.exclude");
        columnsToExclude = getColumnsToExclude(columnsToExcludeString);
        addIntercept = job.getBoolean("add.intercept", false);
        rho = job.getFloat("rho", DEFAULT_RHO);
        regularizationFactor = job.getFloat("regularization.factor", DEFAULT_REGULARIZATION_FACTOR);
        previousIntermediateOutputLocation = job.get("previous.intermediate.output.location");
        previousIntermediateOutputLocationPath = new Path(previousIntermediateOutputLocation);

        try {
            fs = FileSystem.get(job);
        }
        catch (IOException e) {
            LOG.log(Level.FINE, e.toString());
        }

        splitToParameters = getSplitParameters();
    }

    protected Map<String, String> getSplitParameters() {
        return readParametersFromHdfs(fs, previousIntermediateOutputLocationPath, iteration);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
        LOG.info(String.format("Iteration %d Mapper entered map function", iteration));

        FileSplit split = (FileSplit) reporter.getInputSplit();
        LOG.info(String.format("Iteration %d Mapper got input split", iteration));

        String splitId = key.get() + "@" + split.getPath();
        LOG.info(String.format("Iteration %d Mapper got splitId %s", iteration, splitId));

        splitId = removeIpFromHdfsFileName(splitId);
        LOG.info(String.format("Iteration %d Mapper removed ip from hdfs file name splitId %s", iteration, splitId));

        double[][] inputSplitData = createMatrixFromDataString(value.toString(), columnsToExclude, addIntercept);
        LOG.info(String.format("Iteration %d Mapper about to do optimization on splitId %s", iteration, splitId));

        AdmmMapperContext mapperContext;
        if (iteration == 0) {
            mapperContext = new AdmmMapperContext(inputSplitData, rho);
        }
        else {
            mapperContext = assembleMapperContextFromCache(inputSplitData, splitId);
        }
        LOG.info(String.format("Iteration %d Mapper about to do optimization on splitId %s", iteration, splitId));
        AdmmReducerContext reducerContext = localMapperOptimization(mapperContext);

        LOG.info(String.format("Iteration %d Mapper outputting splitId %s", iteration, splitId));
        output.collect(ZERO, new Text(splitId + "::" + admmReducerContextToJson(reducerContext)));
    }

    private String arrayToString(double[] array) {
        StringBuilder result = new StringBuilder();
        for(double d: array) {
            result.append(d);
            result.append(", ");
        }
        return result.toString();
    }

    private AdmmReducerContext localMapperOptimization(AdmmMapperContext context) {
        LogisticL2DifferentiableFunction myFunction =
                new LogisticL2DifferentiableFunction(context.getA(),
                        context.getB(),
                        context.getRho(),
                        context.getUInitial(),
                        context.getZInitial());
        LOG.info(String.format("Rho value: %f", context.getRho()));
        LOG.info(String.format("UInitial value: %s", arrayToString(context.getUInitial())));
        LOG.info(String.format("ZInitial value: %s", arrayToString(context.getZInitial())));
        LOG.info(String.format("XInitial value: %s", arrayToString(context.getXInitial())));
        IOptimizer.Ctx optimizationContext = new IOptimizer.Ctx(context.getXInitial());
        bfgs.minimize(myFunction, optimizationContext);
        double primalObjectiveValue = myFunction.evaluatePrimalObjective(optimizationContext.m_optimumX);
        LOG.info(String.format("Iteration %d Mapper finished optimization", iteration));
        return new AdmmReducerContext(context.getUInitial(),
                context.getXInitial(),
                optimizationContext.m_optimumX,
                context.getZInitial(),
                primalObjectiveValue,
                context.getRho(),
                regularizationFactor);
    }

    private AdmmMapperContext assembleMapperContextFromCache(double[][] inputSplitData, String splitId) throws IOException {
        if (splitToParameters.containsKey(splitId)) {
            AdmmMapperContext preContext = jsonToAdmmMapperContext(splitToParameters.get(splitId));
            return new AdmmMapperContext(inputSplitData,
                    preContext.getUInitial(),
                    preContext.getXInitial(),
                    preContext.getZInitial(),
                    preContext.getRho(),
                    preContext.getLambdaValue(),
                    preContext.getPrimalObjectiveValue(),
                    preContext.getRNorm(),
                    preContext.getSNorm());
        }
        else {
            LOG.log(Level.FINE, String.format("Key not found. Split ID: %s Split Map: %s", splitId, splitToParameters.toString()));
            throw new IOException(String.format("Key not found.  Split ID: %s Split Map: %s", splitId, splitToParameters.toString()));
        }
    }
}