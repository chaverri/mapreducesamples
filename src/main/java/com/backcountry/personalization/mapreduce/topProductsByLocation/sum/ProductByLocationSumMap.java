package com.backcountry.personalization.mapreduce.topProductsByLocation.sum;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ProductByLocationSumMap extends TableMapper<Text, IntWritable> {

    final static Logger logger = Logger.getLogger(ProductByLocationSumMap.class);

    private String locationColumnFamily;
    private String locationColumnName;
    private static final  IntWritable ONE = new IntWritable(1);

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        locationColumnFamily = context.getConfiguration().get(ProductByLocationSettings.LOCATION_COLUMN_FAMILY.name());
        locationColumnName = context.getConfiguration().get(ProductByLocationSettings.LOCATION_COLUMN_NAME.name());
    }

    @Override
    public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
            throws IOException, InterruptedException {
        try {

            String location = new String(columns.getValue(Bytes.toBytes(locationColumnFamily), Bytes.toBytes(locationColumnName)));

            //Get all orders
            for(byte[] columnName : columns.getFamilyMap(Bytes.toBytes("o")).keySet()){

                String orderKey = new String(columnName);

                String productId = orderKey.split("\\|")[1];

                String reduceKey = location + "|" + productId;
                logger.info("Emitting to reducer, key: " + reduceKey);
                // emit store and qty values
                context.write(new Text(reduceKey), ONE);
            }

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }
}