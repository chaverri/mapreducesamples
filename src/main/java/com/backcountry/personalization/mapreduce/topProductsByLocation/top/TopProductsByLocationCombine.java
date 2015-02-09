package com.backcountry.personalization.mapreduce.topProductsByLocation.top;

import com.backcountry.personalization.mapreduce.topProductsByLocation.sum.ProductByLocationSettings;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.TreeMap;

//calculate the top ten locally
public class TopProductsByLocationCombine extends Reducer<Text, MapWritable, Text, MapWritable> {

    final static Logger logger = Logger.getLogger(TopProductsByLocationCombine.class);

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

            logger.info("Combiner receives key:" + key.toString());

            TreeMap<Integer, MapWritable> topSalesLocal = new TreeMap<Integer, MapWritable>();

            //iterate through all the results and get just the top 10
            for(MapWritable detail : values){

                Integer qty = ((IntWritable)detail.get(countKey)).get();
                topSalesLocal.put(qty, detail);

                if(topSalesLocal.size() > topN){
                    topSalesLocal.remove(topSalesLocal.firstKey());
                }
            }

            logger.info("Sending the local top 10 to the reducer for location:" + key);
            for(MapWritable detail : topSalesLocal.values()){
                logger.info("Sending to reducer key:" + key.toString() + " value:" + detail.toString());

                // sends local top to the reducer
                context.write(key, detail);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

