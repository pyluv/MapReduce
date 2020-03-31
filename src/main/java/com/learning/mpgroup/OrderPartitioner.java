package com.learning.mpgroup;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Classname OrderPartitioner
 * @Description TODO
 * @Date 3/26/2020 9:44 PM
 * @Created by Administrator
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {


        int partition = 2;

        // 2 判断是哪个省
//        if (orderBean.getOrder_id() == 1) {
//            partition = 0;
//        }else if (orderBean.getOrder_id() == 2) {
//            partition = 1;
//        }else if (orderBean.getOrder_id() == 3) {
//            partition = 2;
//        }
        if (orderBean.getOrder_id() == 1) {
            partition = 0;} else partition =1;


        return partition;
    }
}
