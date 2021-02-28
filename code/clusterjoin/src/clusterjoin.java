import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Writable.PairWritable;
import Writable.PointWritable;

public class clusterjoin{
	
	private static final double eps = 1e-10;

	public static class DatainMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	String[] s0 = line.split("POINT\\(");
	    	if (s0.length == 1) return;
	    	line = s0[1].split("\\)")[0];
	    	Text res = new Text(line);
	    	context.write(res, NullWritable.get());
	    }
	}
	public static class DatainReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		public void reduce(Text key, NullWritable value, Context context)  throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	
	
	public static class SampleMapper extends Mapper<LongWritable, Text, PointWritable, NullWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	String[] splits = line.split(" ");
	    	double x = Double.valueOf(splits[0]);
	    	double y = Double.valueOf(splits[1]);
	    	
	    	double p = Math.random();
	    	double samplerate = Double.valueOf(context.getConfiguration().get("samplerate"));
	    	PointWritable pt = new PointWritable(0, x, y, 0, 0);
	    	if (p < samplerate) 
	    		context.write(pt, NullWritable.get());
	    	PointWritable zr = new PointWritable(0, 0., 0., 0, 0);
	    	context.write(zr, NullWritable.get());
	    }
	}
	public static class SampleReducer extends Reducer<PointWritable, NullWritable, PointWritable, NullWritable>{
		public void reduce(PointWritable key, Iterable<NullWritable> value, Context context)  throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	
	public static double dist(PointWritable a, PointWritable b) {
		double dx = a.getx() - b.getx();
		double dy = a.gety() - b.gety();
		return Math.sqrt((dx * dx) + (dy * dy));
	}
	public static boolean filter(PointWritable p, PointWritable home, PointWritable c, double threshold) {
		double dh = dist(p, home);
		double dc = dist(p, c);
		double de = dist(c, home);
		return dc * dc > dh * dh + 4 * threshold * de / 2;
	}
	public static ArrayList<PointWritable> readPoints(String path) throws IOException{
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.176.122.126:9000");
		FileSystem fs = FileSystem.get(conf);
		
		ArrayList<PointWritable> rd = new ArrayList<PointWritable>();
        try {
            FSDataInputStream his = fs.open(new Path(path));
            BufferedReader bf = new BufferedReader(new InputStreamReader(his, "UTF-8"));
            String lineString = null;
            
            if (!fs.exists(new Path(path))) {
            	PointWritable c = new PointWritable(1, 2, 3, 4, 5);
		    	rd.add(c);            	
            }
            
            long idx = 0;
            while ((lineString = bf.readLine()) != null) {
                String[] value = lineString.split("\t");
                if (value.length == 2) {
                	String[] nums = value[1].split(" ");
                    PointWritable p = new PointWritable(Long.valueOf(value[0]), Double.valueOf(nums[0]), 
                    									Double.valueOf(nums[1]), Integer.valueOf(nums[2]), 0);
                    rd.add(p);
                } else {
                	String[] nums = value[0].split(" ");
                    PointWritable p = new PointWritable(idx, Double.valueOf(nums[0]), 
                    									Double.valueOf(nums[1]), Integer.valueOf(nums[2]), 0);
                    rd.add(p);
                }
                idx += 1;
            }
            
            his.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return(rd);
    }  
	public static class ClusterMapper extends Mapper<LongWritable, Text, PointWritable, PointWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] nums = value.toString().split(" ");
	    	double x = Double.valueOf(nums[0]);
	    	double y = Double.valueOf(nums[1]);
	    	PointWritable p = new PointWritable();
	    	p.setx(x); p.sety(y);
	    	
	    	String fpath = context.getConfiguration().get("centerfile") + "/part-r-00000";
	    	ArrayList<PointWritable> centers = readPoints(fpath);
	    	
	    	double threshold = Double.valueOf(context.getConfiguration().get("threshold"));
	    	Iterator<PointWritable> cs = centers.iterator();
	    	PointWritable inner = new PointWritable(key.get(), x, y, 0, 0);
	    	PointWritable outer = new PointWritable(key.get(), x, y, 0, 1);
	    	
	    	int mnp = 0;
	    	double mnd = dist(centers.get(0), inner);
	    	for (int i = 0; i < centers.size(); i += 1) {
	    		if (mnd > dist(centers.get(i), p)) {
	    			mnd = dist(centers.get(i), p);
	    			mnp = i;
	    		}
	    	}
	    	
	    	context.write(centers.get(mnp), inner);
	    	PointWritable homec = (PointWritable) centers.get(mnp).clone(); 
	    	for (int i = 0; i < centers.size(); i += 1) 
	    		if (mnp != i & ! filter(inner, homec, centers.get(i), threshold)){
	    			if (((i + mnp) % 2 != 0) ^ (mnp < i))
	    				context.write(centers.get(i), outer);
	    		}
	    }
	}
	public static class ClusterReducer extends Reducer<PointWritable, PointWritable, PointWritable, PointWritable> {
		public int Hash(PointWritable x, long id, long part) {
			  return (int) ((int)(id % part * id % part + id * 233 % part + 97) % part); 
		}
		public void reduce(PointWritable center, Iterable<PointWritable> value, Context context) throws IOException, InterruptedException {
			ArrayList<PointWritable> points = new ArrayList<PointWritable>(); 
			for (PointWritable v : value) {
				PointWritable pt = (PointWritable) v.clone();
				points.add(pt);
			}
			int num = points.size();
			double pr = Double.valueOf(context.getConfiguration().get("samplerate"));
			int m = (int) (pr * points.size()) + 1;
			
			PointWritable ct = (PointWritable)center.clone();
			ct.settype(m);
			for (int i = 0; i < points.size(); i += 1) {
				int id = Hash(points.get(i), i, m);
				PointWritable pt = (PointWritable) points.get(i).clone();
				pt.setr(id);
				context.write(ct, pt);
			}
		}
	}
	
	
	
	public static class SimilarityMapper extends Mapper<LongWritable, Text, PointWritable, PointWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
	    	String[] nums = values[0].split(" ");
	    	PointWritable c = new PointWritable(0, Double.valueOf(nums[0]), Double.valueOf(nums[1]), 
	    										Integer.valueOf(nums[2]), Integer.valueOf(nums[3]));
	    	nums = values[1].split(" ");
	    	PointWritable p = new PointWritable(0, Double.valueOf(nums[0]), Double.valueOf(nums[1]), 
	    										Integer.valueOf(nums[2]), Integer.valueOf(nums[3]));
	    	
	    	for (int i = p.getr(); i < c.gettype(); i += 1) {
	    		PointWritable cid = (PointWritable) c.clone();
	    		cid.setid(p.getr());
	    		cid.settype(i);
	    		context.write(cid, p);
	    	}
	    	for (int i = 0; i < p.getr(); i += 1) {
	    		PointWritable cid = (PointWritable) c.clone();
	    		cid.setid(i);
	    		cid.settype(p.getr());
	    		if (i != p.getr()) context.write(cid, p);
	    	}
		}
	}
	public static class SimilarityReducer extends Reducer<PointWritable, PointWritable, PairWritable, NullWritable> {
		public void reduce(PointWritable key, Iterable<PointWritable> value, Context context) throws IOException, InterruptedException {
			ArrayList<PointWritable> left = new ArrayList<PointWritable>();
			ArrayList<PointWritable> mid = new ArrayList<PointWritable>();
			ArrayList<PointWritable> right = new ArrayList<PointWritable>();
			for (PointWritable v : value) {
				PointWritable pt = (PointWritable) v.clone();
				if (pt.getr() != key.gettype()) 
					right.add(pt);
				else if (pt.getr() != key.getid()) 
					left.add(pt);
				else 
					mid.add(pt);
			}
			
			Comparator<PointWritable> cp = new Comparator<PointWritable>() {
				@Override
	            public int compare(PointWritable o1, PointWritable o2) {
	                int i = o1.type > o2.type ? 1 : -1;
	                return i;
	            }
			};
			left.sort(cp);
			mid.sort(cp);
			right.sort(cp);
			/*
			for (int i = 0; i < left.size(); i += 1) {
				context.write(new PairWritable(key, left.get(i)), NullWritable.get());
			}
			for (int i = 0; i < mid.size(); i += 1) {
				// context.write(new PairWritable(key, mid.get(i)), NullWritable.get());
			}
			for (int i = 0; i < right.size(); i += 1) {
				PointWritable ot = (PointWritable) right.get(i).clone();
				ot.setr(-ot.getr());
				context.write(new PairWritable(key, ot), NullWritable.get());
			}
			*/
			double threshold = Double.valueOf(context.getConfiguration().get("threshold"));
			for (int i = 0; i < left.size(); i += 1) {
				if (left.get(i).gettype() != 0) break;
				PointWritable inner = left.get(i);
				for (int j = right.size() - 1; j >= 0; j -= 1) {
					PointWritable outer = right.get(j);
					if (dist(inner, outer) < threshold) {
						PairWritable res = new PairWritable(inner, outer);
						context.write(res, NullWritable.get());
					}
				}
			}
			for (int i = 0; i < right.size(); i += 1) {
				if (right.get(i).gettype() != 0) break;
				PointWritable inner = right.get(i);
				for (int j = left.size() - 1; j >= 0; j -= 1) {
					PointWritable outer = left.get(j);
					if (outer.gettype() != 1) break;
					if (dist(inner, outer) < threshold) {
						PairWritable res = new PairWritable(inner, outer);
						context.write(res, NullWritable.get());
					}
				}
			}
			
			for (int i = 0; i < mid.size(); i += 1) {
				if (mid.get(i).gettype() != 0) break;
				PointWritable inner = mid.get(i);
				for (int j = i + 1; j < mid.size(); j += 1) {
					PointWritable outer = mid.get(j);
					if (dist(inner, outer) < threshold){
						PairWritable res = new PairWritable(inner, outer);
						context.write(res, NullWritable.get());
					}
				}
			}
			
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration confdatain = new Configuration();
		// confdatain.set("mapreduce.input.fileinputformat.split.maxsize", "67108864");
		Job datain = Job.getInstance(confdatain, "datain");
		// datain.setNumReduceTasks(10);
		datain.setJarByClass(clusterjoin.class);
		datain.setMapperClass(DatainMapper.class);
		datain.setReducerClass(DatainReducer.class);
		datain.setMapOutputKeyClass(Text.class);
		datain.setMapOutputValueClass(NullWritable.class);
		datain.setOutputKeyClass(Text.class);
		datain.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(datain, new Path(args[2]));
        FileOutputFormat.setOutputPath(datain, new Path(args[6]));
        ControlledJob ctrldatain = new ControlledJob(confdatain);
        ctrldatain.setJob(datain);  
		
		
		
		Configuration confsample = new Configuration();
		confsample.set("samplerate", args[1]);
		confsample.set("mapreduce.input.fileinputformat.split.maxsize", "1048576"); 
		Job sample = Job.getInstance(confsample, "sample");
		sample.setJarByClass(clusterjoin.class);
		sample.setMapperClass(SampleMapper.class);
		sample.setReducerClass(SampleReducer.class);
		sample.setOutputKeyClass(PointWritable.class);
		sample.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(sample, new Path(args[6] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(sample, new Path(args[3]));
        
        ControlledJob ctrlsample = new ControlledJob(confsample);
        ctrlsample.setJob(sample);                
        
        
        
        Configuration confcluster = new Configuration();
        confcluster.set("threshold", args[0]);
        confcluster.set("centerfile", args[3]);
        confcluster.set("samplerate", args[1]);
        confcluster.set("mapreduce.input.fileinputformat.split.maxsize", "393216");
        Job cluster = Job.getInstance(confcluster, "cluster");
        cluster.setNumReduceTasks(16);
        cluster.setJarByClass(clusterjoin.class);
        cluster.setMapperClass(ClusterMapper.class);
        cluster.setReducerClass(ClusterReducer.class);
        cluster.setMapOutputKeyClass(PointWritable.class);
        cluster.setMapOutputValueClass(PointWritable.class);
        cluster.setOutputKeyClass(PointWritable.class);
        cluster.setOutputValueClass(PointWritable.class);
        FileInputFormat.setInputPaths(cluster, new Path(args[6] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(cluster, new Path(args[4]));
        
        ControlledJob ctrlcluster = new ControlledJob(confcluster);
        ctrlcluster.setJob(cluster); 
        
        
        
        Configuration confsimilarity = new Configuration();
        confsimilarity.set("threshold", args[0]);
        confsimilarity.set("mapreduce.input.fileinputformat.split.maxsize", "268435456");
        Job similarity = Job.getInstance(confsimilarity, "similarity");
        similarity.setNumReduceTasks(30);
        similarity.setJarByClass(clusterjoin.class);
        similarity.setMapperClass(SimilarityMapper.class);
        similarity.setReducerClass(SimilarityReducer.class);
        similarity.setMapOutputKeyClass(PointWritable.class);
        similarity.setMapOutputValueClass(PointWritable.class);
        similarity.setOutputKeyClass(PairWritable.class);
		similarity.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(similarity, new Path(args[4]));
        FileOutputFormat.setOutputPath(similarity, new Path(args[5]));
        
        ControlledJob ctrlsimilarity = new ControlledJob(confsimilarity);
        ctrlsimilarity.setJob(similarity);
        
        // System.exit(similarity.waitForCompletion(true) ? 0 : 1);
        
        
        
        ctrlsample.addDependingJob(ctrldatain);
        ctrlcluster.addDependingJob(ctrlsample);
        ctrlsimilarity.addDependingJob(ctrlcluster);
        
        JobControl allcontroller = new JobControl("AllController");
        allcontroller.addJob(ctrldatain);
        allcontroller.addJob(ctrlsample);
        allcontroller.addJob(ctrlcluster);
        allcontroller.addJob(ctrlsimilarity);
        
        Thread ctrlthread = new Thread(allcontroller);
        ctrlthread.start();
        while (true) {
            if (allcontroller.allFinished()) {
                System.out.println(allcontroller.getSuccessfulJobList());
                allcontroller.stop();
                break;
            }
        }
        
	}
}
