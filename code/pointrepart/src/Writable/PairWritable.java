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

    //比较
    public int compareTo(PairWritable that) {
        if (this.x.compareTo(that.x) != 0) {
        	return this.x.compareTo(that.x);
        } else 
        	return this.y.compareTo(that.y);
    }
    //序列化--dataOutput(data流)：可以自定义序列化对象，节省空间，hadoop用的就是这个流
    public void write(DataOutput out) throws IOException {
        this.x.write(out);
        this.y.write(out);
    }
    //反序列化
    public void readFields(DataInput in) throws IOException {
        this.x.readFields(in);
    	this.y.readFields(in);
    }
}
