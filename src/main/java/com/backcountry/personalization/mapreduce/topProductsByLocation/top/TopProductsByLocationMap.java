package com.backcountry.personalization.mapreduce.topProductsByLocation.top;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TopProductsByLocationMap extends TableMapper<Text, MapWritable> {

    final static Logger logger = Logger.getLogger(TopProductsByLocationMap.class);

    private static final Text locationKey =  new Text("location");
    private static final Text productKey =  new Text("productId");
    private static final Text countKey =  new Text("count");

    @Override
    public void setup(Context context) throws IOException, InterruptedException{

    }

    @Override
    public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
            throws IOException, InterruptedException {
        try {


            String location = new String(columns.getValue(Bytes.toBytes("cfInfo"), Bytes.toBytes("Location")));
            String productId = new String(columns.getValue(Bytes.toBytes("cfInfo"), Bytes.toBytes("ProductId")));
            Integer count = Bytes.toInt(columns.getValue(Bytes.toBytes("cfInfo"), Bytes.toBytes("Count")));

            MapWritable details = new MapWritable();
            details.put(locationKey, new Text(location));
            details.put(productKey, new Text(productId));
            details.put(countKey, new IntWritable(count));


            logger.info("Emitting to reducer, key(location):" + location + ", productId:" + productId + " count:" + count);
            // emit location(state, city, etc) and qty values
            context.write(new Text(location), details);

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }
}