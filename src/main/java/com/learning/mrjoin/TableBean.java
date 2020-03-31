package com.learning.mrjoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Classname TableBean
 * @Description TODO
 * @Date 3/27/2020 2:00 PM
 * @Created by Administrator
 */
public class TableBean implements WritableComparable<TableBean> {
    private String order_id; // 订单id
    private String p_id;      // 产品id
    private int amount;       // 产品数量
    private String pname;     // 产品名称
    private String flag;      // 表的标记

    public TableBean() {
        super();
    }

    @Override
    public int compareTo(TableBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(order_id);
        dataOutput.writeUTF(p_id);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readUTF();
        p_id = dataInput.readUTF();
        amount = dataInput.readInt();
        pname = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return order_id + "\t" + pname + "\t" + amount + "\t";
    }
}
