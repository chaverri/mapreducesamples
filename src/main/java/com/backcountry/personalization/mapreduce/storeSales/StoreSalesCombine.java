package com.backcountry.personalization.mapreduce.storeSales;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class StoreSalesCombine extends Reducer<Text, IntWritable, Text, IntWritable> {

    final static Logger logger = Logger.getLogger(StoreSalesCombine.class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        try {

            logger.info("Combiner receives key:" + key.toString());

            int sum = 0;

            // loop through different sales vales by key (store) and add it to sum
            logger.info("With these values: ");
            for (IntWritable sales : values) {
                logger.info("\t" + sales.toString());
                Integer intSales = new Integer(sales.toString());
                sum += intSales;
            }

            logger.info("Sending to reducer (after sum) key:" + key.toString() + " value:" + sum);

            // write data to Hbase table
            context.write(key, new IntWritable(sum));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

