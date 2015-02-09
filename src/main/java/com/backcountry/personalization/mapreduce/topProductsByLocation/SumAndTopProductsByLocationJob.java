package com.backcountry.personalization.mapreduce.topProductsByLocation;

import com.backcountry.personalization.mapreduce.topProductsByLocation.sum.ProductsByLocationSumJob;
import com.backcountry.personalization.mapreduce.topProductsByLocation.top.TopProductsByLocationJob;

import java.io.IOException;

public class SumAndTopProductsByLocationJob {

    public boolean run(String location, int top) throws IOException, ClassNotFoundException, InterruptedException{

        ProductsByLocationSumJob productsByLocationSumJob = new ProductsByLocationSumJob();
        TopProductsByLocationJob topProductsByLocationJob = new TopProductsByLocationJob();

        boolean result = false;

        //result = productsByLocationSumJob.run(location);

        //if(result){
        //    result = topProductsByLocationJob.run(top, location);
        //}

        return result;
    }
}


/*

put 'orders','order1#Line1', 'cfInfo:ProductId','Product1'
put 'orders','order1#Line1', 'cfInfo:Qty', 1
put 'orders','order1#Line1', 'cfInfo:State', 'FL'
put 'orders','order1#Line1', 'cfInfo:City', 'MIAMI'

*/
