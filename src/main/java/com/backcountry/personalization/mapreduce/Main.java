package com.backcountry.personalization.mapreduce;

import com.backcountry.personalization.mapreduce.topProductsByLocation.SumAndTopProductsByLocationJob;

public class Main {

    public static void main(String[] args) throws Exception {

        SumAndTopProductsByLocationJob sumAndTopProductsByLocationJob = new SumAndTopProductsByLocationJob();
        sumAndTopProductsByLocationJob.run("City", 5);
        //sumAndTopProductsByLocationJob.run("State", 5);

    }
}
