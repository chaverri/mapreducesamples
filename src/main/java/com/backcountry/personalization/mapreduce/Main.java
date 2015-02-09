package com.backcountry.personalization.mapreduce;

import com.backcountry.personalization.mapreduce.topProductsByLocation.sum.ProductsByLocationSumJob;

public class Main {

    public static void main(String[] args) throws Exception {

        ProductsByLocationSumJob productsByLocationSumJob = new ProductsByLocationSumJob();
        productsByLocationSumJob.run("m","s");

    }
}
