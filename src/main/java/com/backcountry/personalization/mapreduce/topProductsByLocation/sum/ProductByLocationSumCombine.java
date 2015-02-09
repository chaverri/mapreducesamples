package com.backcountry.personalization.mapreduce.topProductsByLocation.sum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ProductByLocationSumCombine extends Reducer<Text, IntWritable, Text, IntWritable> {

    final static Logger logger = Logger.getLogger(ProductByLocationSumCombine.class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        try {

            logger.info("Combiner receives key:" + key.toString());

            int count = 0;

            // loop through different sales vales by key (store) and add it to sum
           for (IntWritable productCount : values) {
                count += productCount.get();
            }

            logger.info("Sending to reducer (after count) key:" + key.toString() + " value:" + count);

            // send to reducer
            context.write(key, new IntWritable(count));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


