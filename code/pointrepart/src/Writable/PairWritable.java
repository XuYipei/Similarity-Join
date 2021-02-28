package Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
public class PairWritable implements WritableComparable<PairWritable> {
	private static final double eps = 1e-10; 
    public PointWritable x, y;

    public PairWritable() {
    	this.x = new PointWritable();
    	this.y = new PointWritable();
    }
    
    public PairWritable(PointWritable x, PointWritable y) {
    	this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return x.getx() + " " + x.gety() + "\t" + y.getx() + " " + y.gety();
    }

    public PointWritable getx() {return x;}
    public PointWritable gety() {return y;}
    
    public void setx(PointWritable x) {this.x = x;}
    public void sety(PointWritable y) {this.y = y;}

    //�Ƚ�
    public int compareTo(PairWritable that) {
        if (this.x.compareTo(that.x) != 0) {
        	return this.x.compareTo(that.x);
        } else 
        	return this.y.compareTo(that.y);
    }
    //���л�--dataOutput(data��)�������Զ������л����󣬽�ʡ�ռ䣬hadoop�õľ��������
    public void write(DataOutput out) throws IOException {
        this.x.write(out);
        this.y.write(out);
    }
    //�����л�
    public void readFields(DataInput in) throws IOException {
        this.x.readFields(in);
    	this.y.readFields(in);
    }
}
