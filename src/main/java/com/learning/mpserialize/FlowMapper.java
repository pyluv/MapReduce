package com.learning.mpserialize;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    FlowBean v = new FlowBean();
    Text k = new Text();


    @Override
    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String[] value = values.toString().split("\t");
        String phontNum = value[1];
        long upFlow = Long.parseLong(value[3]);
        long downFlow = Long.parseLong(value[4]);
        k.set(phontNum);
        v.setDownFlow(downFlow);
        v.setUpFlow(upFlow);

        context.write(k, v);


    }
}
