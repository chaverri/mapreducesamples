package com.backcountry.personalization.mapreduce.topProductsByLocation.top;


import com.backcountry.personalization.mapreduce.topProductsByLocation.sum.ProductByLocationSettings;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.TreeMap;

public class TopProductsByLocationReduce extends TableReducer<Text, MapWritable, ImmutableBytesWritable> {

    final static Logger logger = Logger.getLogger(TopProductsByLocationReduce.class);

    private int topN;
    private Text countKey = new Text("count");

    @Override
    public void setup(Context context) throws IOException{
        topN = context.getConfiguration().getInt(ProductByLocationSettings.TOP_N.name(), 10);
    }

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        try {

            logger.info("Reducer receives key:" + key.toString());

            TreeMap<Integer, MapWritable> topSalesLocal = new TreeMap<Integer, MapWritable>();

            //iterate through all the results and get just the top 10
            for(MapWritable detail : values){

                Integer qty = ((IntWritable)detail.get(countKey)).get();
                topSalesLocal.put(qty, detail);

                if(topSalesLocal.size() > topN){
                    topSalesLocal.remove(topSalesLocal.firstKey());
                }
            }

            logger.info("Sending the global top 10 to the reducer for location:" + key.toString());

            // create hbase put with rowkey as location
            Put insHBase = new Put(key.getBytes());
            int productIndex = 1;

            for(MapWritable detail : topSalesLocal.descendingMap().values()){
                logger.info("Saving into hbase key:" + key.toString() + " value:" + detail.toString());

                // insert sum value to hbase
                insHBase.add(Bytes.toBytes("cfInfo"), Bytes.toBytes("Product" + productIndex), Bytes.toBytes(((IntWritable)detail.get(countKey)).get()));

                productIndex++;
            }

            // sends local top to the reducer
            context.write(null, insHBase);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
