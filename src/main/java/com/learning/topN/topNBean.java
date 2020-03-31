package com.learning.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Classname topNBean
 * @Description TODO
 * @Date 3/30/2020 10:34 AM
 * @Created by Administrator
 */
public class topNBean implements WritableComparable<topNBean> {

    private long phoneNum;
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public topNBean() {
        super();
    }

    public topNBean(long phoneNum, long upFlow, long downFlow, long sumFlow) {
        super();
        this.phoneNum = phoneNum;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(topNBean bean) {
        int result = 0;
        if (bean.getSumFlow() > getSumFlow()) {
            result = 1;
        } else result = -1;
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(phoneNum);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneNum = dataInput.readLong();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    public long getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(long phoneNum) {
        this.phoneNum = phoneNum;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return phoneNum +
                "\t" + upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }
}
