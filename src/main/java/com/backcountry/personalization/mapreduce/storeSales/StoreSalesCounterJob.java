package com.backcountry.personalization.mapreduce.storeSales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class StoreSalesCounterJob {
    final static Logger logger = Logger.getLogger(StoreSalesCounterJob.class);

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Cluster cluster = new Cluster(conf);

        Job job = Job.getInstance(cluster, conf);
        logger.info("Initializing job ...");

        job.setJarByClass(StoreSalesCounterJob.class); // class that contains mapper and reducer
        Scan scan = new Scan();
        scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        logger.info("Setting mapper ...");
        // set other scan attrs
        TableMapReduceUtil.initTableMapperJob(
                "sales", // input table
                scan, // Scan instance to control CF and attribute selection
                StoreSalesCounter.class, // mapper class
                Text.class, // mapper output key
                IntWritable.class, // mapper output value
                job);

        Path outputDir = new Path("/tmp/salesResult");
        FileOutputFormat.setOutputPath(job, outputDir);

        logger.info("Running job ...");
        boolean result = job.waitForCompletion(true);

        if(result){
            for(Counter counter : job.getCounters().getGroup(StoreSalesCounter.STORE_SALES_COUNTER_GROUP)){
                System.out.println(counter.getDisplayName() + " \t" + counter.getValue());
            }
        }


        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);

        logger.info("Job Finished !");
        return result;
    }
}
