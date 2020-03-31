package com.learning.topN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Classname topNReducer
 * @Description TODO
 * @Date 3/30/2020 10:47 AM
 * @Created by Administrator
 */
public class topNReducer extends Reducer <topNBean, NullWritable, topNBean, NullWritable> {
    @Override
    protected void reduce(topNBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());

    }
}
