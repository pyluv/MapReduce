package com.learning.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;


/**
 * @Classname topNMapper
 * @Description TODO
 * @Date 3/30/2020 10:40 AM
 * @Created by Administrator
 */
public class topNMapper extends Mapper<LongWritable, Text, topNBean, NullWritable> {


    private TreeMap<Long, topNBean> flowMap = new TreeMap<Long, topNBean>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        topNBean bean = new topNBean();

        String[] fileds = value.toString().split("\t");
        bean.setPhoneNum(Long.parseLong(fileds[0]));
        bean.setUpFlow(Long.parseLong(fileds[1]));
        bean.setDownFlow(Long.parseLong(fileds[2]));
        bean.setSumFlow(Long.parseLong(fileds[3]));

        // 4 向TreeMap中添加数据
        flowMap.put(bean.getSumFlow(), bean);

        // 5 限制TreeMap的数据量，超过10条就删除掉流量最小的一条数据
        if (flowMap.size() > 10) {
		flowMap.remove(flowMap.firstKey());
            //flowMap.remove(flowMap.lastKey());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<Long> sum = flowMap.keySet().iterator();

        while (sum.hasNext()) {
            Long k = sum.next();
            context.write(flowMap.get(k), NullWritable.get());
        }

    }
}
