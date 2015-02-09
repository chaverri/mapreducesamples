package com.backcountry.personalization.mapreduce.topProductsByLocation.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ProductsByLocationSumJob {

    final static Logger logger = Logger.getLogger(ProductsByLocationSumJob.class);

    public boolean run(String location) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config, "ProductsByLocationSumJob");

        logger.info("Initializing job ...");

        job.setJarByClass(ProductsByLocationSumJob.class); // class that contains mapper and reducer
        Scan scan = new Scan();
        scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        logger.info("Setting mapper ...");
        // set other scan attrs
        TableMapReduceUtil.initTableMapperJob(
                "orders", // input table
                scan, // Scan instance to control CF and attribute selection
                ProductByLocationSumMap.class, // mapper class
                Text.class, // mapper output key
                IntWritable.class, // mapper output value
                job);

        logger.info("Setting reducer...");
        TableMapReduceUtil.initTableReducerJob(
                "purchaseBy" + location, // output table
                ProductByLocationSumReduce.class, // reducer class
                job);

        logger.info("Setting combiner ...");
        job.setCombinerClass(ProductByLocationSumCombine.class);

        job.setNumReduceTasks(10); // at least one, adjust as required

        job.getConfiguration().set(ProductByLocationSettings.LOCATION_COLUMN_FAMILY.name(), "cfInfo");
        job.getConfiguration().set(ProductByLocationSettings.LOCATION_COLUMN_NAME.name(), location);


        logger.info("Running job ...");
        boolean result = job.waitForCompletion(true);

        logger.info("Job Finished !");
        return result;
    }
}