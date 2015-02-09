package com.backcountry.personalization.mapreduce.storeSales;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;


public class StoreSalesReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    final static Logger logger = Logger.getLogger(StoreSalesReduce.class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        try {

            logger.info("Reducer receives key:" + key.toString());

            int sum = 0;

            // loop through different sales vales by key (store) and add it to sum
            logger.info("With these values: ");
            for (IntWritable sales : values) {
                logger.info("\t" + sales.toString());
                Integer intSales = new Integer(sales.toString());
                sum += intSales;
            }

            // create hbase put with rowkey as store
            Put insHBase = new Put(key.getBytes());

            // insert sum value to hbase
            insHBase.add(Bytes.toBytes("cfAggregateSales"), Bytes.toBytes("AggregateSales"), Bytes.toBytes(sum));

            logger.info("Saving (after sum) key:" + key.toString() + " value:" + sum);

            // write data to Hbase table
            context.write(null, insHBase);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
