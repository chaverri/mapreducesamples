package com.backcountry.personalization.mapreduce.storeSales;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

// USE COUNTER WHEN THE POSSIBLE KEYS ARE JUST VALUES BELOW 100
public class StoreSalesCounter extends TableMapper<Text, IntWritable> {

    final static Logger logger = Logger.getLogger(StoreSalesCounter.class);
    public static final String STORE_SALES_COUNTER_GROUP="StoreSales";

    @Override
    public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
            throws IOException, InterruptedException {
        try {

            // get rowKey and convert it to string
            String inKey = new String(rowKey.get());


            // set new key having only store
            String storeKey = inKey.split("#")[0];

            // get sales column in byte format first and then convert it to string (as it is stored as string from hbase shell)
            byte[] bSales = columns.getValue(Bytes.toBytes("cfSales"), Bytes.toBytes("Sales"));

            String AggregateSales = new String(bSales);
            Integer sales = new Integer(AggregateSales);

            logger.info("Input key:" + inKey + " sale:" + sales);

            logger.info("Increasing counter:" + storeKey + " with value:" + sales);

            context.getCounter(STORE_SALES_COUNTER_GROUP, storeKey).increment(sales);

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }
}


