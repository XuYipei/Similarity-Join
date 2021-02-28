package Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
public class PointWritable implements WritableComparable<PointWritable>, Cloneable {
	private static final double eps = 1e-10; 
	
    public long id;
    public double x, y;
    public int r, type;

    public PointWritable() {
    	this.id = 0;
    	this.x = 0;
    	this.y = 0;
    	this.r = 0;
    	this.type = 0;
    }
    
    public PointWritable(long id, double x, double y, int r, int type) {
    	this.id = id;
    	this.x = x;
        this.y = y;
        this.r = r;
        this.type = type;
    }

    @Override
    public String toString() {
        return x + " " + y + " " + r + " " + type;
    }

    public double getx() {return x;}
    public double gety() {return y;}
    public int getr() {return r;}
    public long getid() {return id;}
    public int gettype() {return type;}
    
    public void setx(double x) {this.x = x;}
    public void sety(double y) {this.y = y;}
    public void setr(int r) {this.r = r;}
    public void setid(long id) {this.id = id;}
    public void settype(int type) {this.type = type;}

    //比较
    public int compareTo(PointWritable that) {
        //先比较总成绩
        if (this.type < that.gettype()) {
        	return 1;
        } else if (this.type > that.gettype()) {
            return -1;
        } else if (this.id < that.getid()) {
            return 1;
        } else if (this.id > that.getid()) {
            return -1;
        } else if (this.x + eps < that.getx()) {
            return 1; 
        } else if (this.x - eps > that.getx()) {
        	return -1;
        } else if (this.y + eps < that.gety()) {
        	return 1;
        } else if (this.y - eps > that.gety()) {
        	return -1;
        } else if (this.r < that.getr()) {
        	return 1;
        } else if (this.r > that.getr()) {
        	return -1;
        } else 
        	return 0;
    }
    //序列化--dataOutput(data流)：可以自定义序列化对象，节省空间，hadoop用的就是这个流
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeDouble(x);
        out.writeDouble(y);
        out.writeInt(r);
        out.writeInt(type);
    }
    //反序列化
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.x = in.readDouble();
        this.y = in.readDouble();
        this.r = in.readInt();
        this.type = in.readInt();
    }
    
    @Override
    public Object clone() {
        try {
            PointWritable dc = (PointWritable) super.clone();
            return dc;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

}
