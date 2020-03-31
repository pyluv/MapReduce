package com.learning.mpsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartitioner extends Partitioner <FlowBean, Text> {


    @Override
    public int getPartition(FlowBean flowBean, Text phoneNum, int numPartitions) {
        String preNum = phoneNum.toString().substring(0, 3);

        int partition = 5;

        // 2 判断是哪个省
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }else if ("135".equals(preNum)) {
            partition = 4;
        }

        return partition;
    }
}
