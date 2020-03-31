package com.learning.mpgroup;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable v : values) {
            context.write(key, NullWritable.get());
        }
       // context.write(key, NullWritable.get());

    }
}
