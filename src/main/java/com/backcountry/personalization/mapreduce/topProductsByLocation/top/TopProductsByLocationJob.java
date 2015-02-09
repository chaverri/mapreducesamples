package com.backcountry.personalization.mapreduce.topProductsByLocation.top;

import com.backcountry.personalization.mapreduce.topProductsByLocation.sum.ProductByLocationSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TopProductsByLocationJob {

    final static Logger logger = Logger.getLogger(TopProductsByLocationJob.class);

    public boolean run(Integer top, String location) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config, "TopProductsByLocationJob");

        logger.info("Initializing job ...");

        job.setJarByClass(TopProductsByLocationJob.class); // class that contains mapper and reducer
        Scan scan = new Scan();
        scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        logger.info("Setting mapper ...");
        // set other scan attrs
        TableMapReduceUtil.initTableMapperJob(
                "purchaseBy" + location, // input table
                scan, // Scan instance to control CF and attribute selection
                TopProductsByLocationMap.class, // mapper class
                Text.class, // mapper output key
                MapWritable.class, // mapper output value
                job);

        logger.info("Setting reducer...");
        TableMapReduceUtil.initTableReducerJob(
                "topPurchaseBy" + location, // output table
                TopProductsByLocationReduce.class, // reducer class
                job);

        logger.info("Setting combiner ...");
        job.setCombinerClass(TopProductsByLocationCombine.class);

        job.setNumReduceTasks(10); // at least one, adjust as required

        //Sends to the job the top number to calculate
        job.getConfiguration().set(ProductByLocationSettings.TOP_N.name(), top.toString());

        logger.info("Running job ...");
        boolean result = job.waitForCompletion(true);

        logger.info("Job Finished !");
        return result;
    }
}
