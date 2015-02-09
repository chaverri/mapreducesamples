package com.backcountry.personalization.mapreduce.storeSales;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.TreeMap;


//If top ten is GLOBAL it can be done on the mapper, is not it can be done with combiners and reducers
public class TopStoreSalesMapper extends TableMapper<NullWritable, StoreSalesInfo> {
    final static Logger logger = Logger.getLogger(TopStoreSalesMapper.class);

    public static final String TOP_N_SETTING = "TOP_N";
    private int topN;
    private TreeMap<Integer, StoreSalesInfo> topSalesLocal = new TreeMap<Integer, StoreSalesInfo>();

    @Override
    public void setup(Context context) throws IOException{
        topN = context.getConfiguration().getInt(TOP_N_SETTING, 10);
    }

    @Override
    public void map(ImmutableBytesWritable rowKey, Result columns, Mapper.Context context)
            throws IOException, InterruptedException {
        try {

            // get rowKey and convert it to string
            String inKey = new String(rowKey.get());

            // set new key having only store
            String storeKey = inKey.split("#")[0];
            String item = inKey.split("#")[1];

            // get sales column in byte format first and then convert it to string (as it is stored as string from hbase shell)
            byte[] bSales = columns.getValue(Bytes.toBytes("cfSales"), Bytes.toBytes("Sales"));

            String AggregateSales = new String(bSales);
            Integer sales = new Integer(AggregateSales);

            logger.info("Input key:" + inKey + " sale:" + sales);

            StoreSalesInfo info = new StoreSalesInfo();
            info.setItem(item);
            info.setSales(sales);
            info.setStore(storeKey);

            topSalesLocal.put(sales, info);

            if(topSalesLocal.size() > topN){
                topSalesLocal.remove(topSalesLocal.firstKey());
            }

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(StoreSalesInfo info : topSalesLocal.values()){
            context.write(NullWritable.get(), info);
        }
    }

}




/*

put 'orders','order1#Line1', 'cfInfo:ProductId','Product1'
put 'orders','order1#Line1', 'cfInfo:Qty', 1
put 'orders','order1#Line1', 'cfInfo:State', 'FL'
put 'orders','order1#Line1', 'cfInfo:City', 'MIAMI'


* */
