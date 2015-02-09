package com.backcountry.personalization.mapreduce.topProductsByLocation.sum;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ProductByLocationSumReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    final static Logger logger = Logger.getLogger(ProductByLocationSumReduce.class);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        try {

            logger.info("Reducer receives key:" + key.toString());

            String inKey = new String(key.getBytes());
            // set new key having only store
            String location = inKey.split(":")[0];
            String productId = inKey.split(":")[1];

            Integer count = 0;

            // loop through different sales vales by key (location and product) and add it to sum
            for (IntWritable productCount : values) {
                count += productCount.get();
            }

            // create hbase put with rowkey as store
            Put insHBase = new Put(key.getBytes());

            insHBase.add(Bytes.toBytes("cfInfo"), Bytes.toBytes("Location"), Bytes.toBytes(location));
            insHBase.add(Bytes.toBytes("cfInfo"), Bytes.toBytes("ProductId"), Bytes.toBytes(productId));

            // insert count value to hbase
            insHBase.add(Bytes.toBytes("cfInfo"), Bytes.toBytes("Count"), Bytes.toBytes(count));

            logger.info("Saving (after sum) key:" + key.toString() + " value:" + count);

            // write data to Hbase table
            context.write(null, insHBase);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

