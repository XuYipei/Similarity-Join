import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import Writable.PairWritable;
import Writable.PointWritable;

public class pointrepart{
	
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
                    PointWritable p = new PointWritable(0, Double.valueOf(nums[0]), 
                    									Double.valueOf(nums[1]), Double.valueOf(nums[2]), 0);
                    rd.add(p);
                } else {
                	String[] nums = value[0].split(" ");
                    PointWritable p = new PointWritable(0, Double.valueOf(nums[0]), 
                    									Double.valueOf(nums[1]), Double.valueOf(nums[2]), 0);
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
	public static class ClusterMapper extends Mapper<LongWritable, Text, PointWritable, DoubleWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] nums = value.toString().split(" ");
	    	double x = Double.valueOf(nums[0]);
	    	double y = Double.valueOf(nums[1]);
	    	
	    	String fpath = context.getConfiguration().get("centerfile") + "/part-r-00000";
	    	ArrayList<PointWritable> centers = readPoints(fpath);
	    	
	    	DoubleWritable mnd = new DoubleWritable(1e10);
	    	PointWritable mnp = new PointWritable();
	    	Iterator<PointWritable> cs = centers.iterator();
	    	while (cs.hasNext()) {
	    		PointWritable c = cs.next();
		    	double dis = Math.sqrt((x - c.getx()) * (x - c.getx()) + (y - c.gety()) * (y - c.gety()));
		    	if (dis < mnd.get()) {
		    		mnd.set(dis);
		    		mnp = (PointWritable) c.clone(); 
		    	}
		    }
	    	context.write(mnp, mnd);
	    }
	} 
	public static class ClusterReducer extends Reducer<PointWritable, DoubleWritable, PointWritable, NullWritable> {
		public void reduce(PointWritable key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
			PointWritable c = new PointWritable(key.getid(), key.getx(), key.gety(), 0, 0);
			for (DoubleWritable d : value) {
				c.setr(Math.max(c.getr(), d.get()));
			}
	    	context.write(c, NullWritable.get());
		}
	}

	
	
	public static class similarityMapper extends Mapper<LongWritable, Text, PointWritable, PointWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] nums = value.toString().split(" ");
	    	double x = Double.valueOf(nums[0]);
	    	double y = Double.valueOf(nums[1]);
	    	PointWritable pt = new PointWritable(0, x, y, 0, 0);
	    	
	    	String fpath = context.getConfiguration().get("centerfile") + "/part-r-00000";
	    	ArrayList<PointWritable> centers = readPoints(fpath);
	    	
	    	double m = Double.valueOf(context.getConfiguration().get("threshold"));
	    	Iterator<PointWritable> cs = centers.iterator();
	    	
	    	double mnd = dist(centers.get(0), pt);
	    	int mnp = 0;
	    	for (int i = 0; i < centers.size(); i += 1) {
	    		PointWritable c = centers.get(i);
	    		if (dist(c, pt) < mnd) {
	    			mnd = dist(c, pt); mnp = i;
	    		}
	    	}
	    	
	    	for (int i = 0; i < centers.size(); i += 1) {
	    		PointWritable c = centers.get(i);
	    		double dis = dist(c, pt);
	    		if (i == mnp) {
	    			PointWritable p = new PointWritable(key.get(), x, y, 0, 0);
	    			context.write(c, p);
	    		} else if (mnp != i & c.getr() + m + eps > dist(c, pt)){
	    			if (((i + mnp) % 2 != 0) ^ (mnp < i)) {
	    				PointWritable p = new PointWritable(key.get(), x, y, 0, 1);
		    			context.write(c, p);
	    			}
	    				
	    		}
	    	}
	    }
	}
	public static class repartitionReducer extends Reducer<PointWritable, PointWritable, PointWritable, PointWritable> {
		
		private MultipleOutputs<PointWritable,PointWritable> mos;  
		public void setup(Context context) throws IOException,InterruptedException {  
			mos = new MultipleOutputs(context);  
		}  
		public void cleanup(Context context) throws IOException,InterruptedException {  
			mos.close();  
		}
		
		public void reduce(PointWritable key, Iterable<PointWritable> value, Context context) throws IOException, InterruptedException {
			ArrayList<PointWritable> points = new ArrayList<PointWritable>();
			for (PointWritable pt : value) {
				PointWritable pt_ = (PointWritable) pt.clone();
				points.add(pt_);
			}
			double m = Double.valueOf(context.getConfiguration().get("threshold"));
			double samplerate = Double.valueOf(context.getConfiguration().get("samplerate"));
			
			/*
			for (int i = 0; i < points.size(); i += 1) {
				context.write(key, points.get(i));
			}
			*/
			
			
			ArrayList<PointWritable> centers = new ArrayList<PointWritable>();
			centers.add(key);
			if ((double)(points.size()) * samplerate > 1) {
				for (int i = 0; i < points.size(); i += 1) {
					double p = Math.random();
					PointWritable ct = (PointWritable) (points.get(i)).clone();
			    	if (p < samplerate) centers.add(ct);
				}
			}
			
			for (int i = 0; i < points.size(); i += 1) {
				PointWritable p = points.get(i);
				double mnd = 1e10;
				int mnp = 0;
				if (p.gettype() == 1) continue;
				for (int j = 0; j < centers.size(); j += 1) {
					PointWritable c = centers.get(j);
					if (mnd > dist(p, c)) {
						mnd = dist(p, c); mnp = j;
					}
				}
				double mr = Math.max(centers.get(mnp).getr(), mnd);
				centers.get(mnp).setr(mr);
			}
	    	for (int i = 0; i < points.size(); i += 1) {
	    		PointWritable p = points.get(i);
	    		if (p.gettype() == 1) {
	    			for (int j = 0; j < centers.size(); j += 1) 
			    		if (centers.get(j).getr() + m + eps > dist(centers.get(j), p)){
			    			context.write(centers.get(j), p);
			    		}
	    			continue;
	    		}
	    		double mnd = dist(centers.get(0), p);
	    		int mnp = 0;
	    		for (int j = 0; j < centers.size(); j += 1) {
		    		PointWritable c = centers.get(j);
		    		if (dist(c, p) < mnd) {
		    			mnd = dist(c, p); mnp = j;
		    		}
		    	}
	    		PointWritable inner = (PointWritable) p.clone(); inner.settype(0);
		    	PointWritable outer = (PointWritable) p.clone(); outer.settype(1);
	    		
	    		context.write(centers.get(mnp), inner);
		    	for (int j = 0; j < centers.size(); j += 1) 
		    		if ((mnp != j) & (centers.get(j).getr() + m + eps > dist(centers.get(j), p))){
		    			if ((((j + mnp) % 2 != 0) ^ (mnp < j)))
		    				context.write(centers.get(j), outer);
		    		}
	    	}
	    	
		}
	}
	
	
	
	public static class repartitionMapper extends Mapper<LongWritable, Text, PointWritable, PointWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] values = value.toString().split("\t");
	    	String[] nums = values[0].split(" ");
	    	PointWritable c = new PointWritable(0, Double.valueOf(nums[0]),
	    										Double.valueOf(nums[1]), Double.valueOf(nums[2]), Integer.valueOf(nums[3]));
	    	nums = values[1].split(" ");
	    	PointWritable p = new PointWritable(0, Double.valueOf(nums[0]),
												Double.valueOf(nums[1]), Double.valueOf(nums[2]), Integer.valueOf(nums[3]));
	    	context.write(c, p);
	    }
	}
	public static class similarityReducer extends Reducer<PointWritable, PointWritable, PairWritable, NullWritable> {
		public void reduce(PointWritable key, Iterable<PointWritable> value, Context context) throws IOException, InterruptedException {
			ArrayList<PointWritable> points = new ArrayList<PointWritable>();
			for (PointWritable pt : value) {
				PointWritable pt_ = (PointWritable) pt.clone();
				points.add(pt_);
			}
			points.sort(new Comparator<PointWritable>() {
				@Override
	            public int compare(PointWritable o1, PointWritable o2) {
	                int i = o1.type > o2.type ? 1 : -1;
	                return i;
	            }
			});
			double m = Double.valueOf(context.getConfiguration().get("threshold"));
			for (int i = 0; i < points.size(); i += 1) {	
				PointWritable inner = points.get(i);
				if (inner.gettype() != 0) break;
				/*
				PairWritable res = new PairWritable(key, inner);
				context.write(res, NullWritable.get());
				*/
				
				for (int j = i + 1; j < points.size(); j += 1) {
					PointWritable outer = points.get(j);
					if (dist(inner, outer) <= m + eps) {
						PairWritable res = new PairWritable(inner, outer);
						context.write(res, NullWritable.get());
					}
				}
				
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration confdatain = new Configuration();
		// confdatain.setLong("mapreduce.job.maps", 6);
		// confdatain.set("mapreduce.input.fileinputformat.split.maxsize", "67108864");
		Job datain = Job.getInstance(confdatain, "datain");
		// datain.setNumReduceTasks(10);
		datain.setJarByClass(pointrepart.class);
		datain.setMapperClass(DatainMapper.class);
		datain.setReducerClass(DatainReducer.class);
		datain.setMapOutputKeyClass(Text.class);
		datain.setMapOutputValueClass(NullWritable.class);
		datain.setOutputKeyClass(Text.class);
		datain.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(datain, new Path(args[2]));
        FileOutputFormat.setOutputPath(datain, new Path(args[7]));
        ControlledJob ctrldatain = new ControlledJob(confdatain);
        ctrldatain.setJob(datain);  
		
		
		
		Configuration confsample = new Configuration();
		confsample.set("samplerate", args[1]);
		confsample.set("mapreduce.input.fileinputformat.split.maxsize", "1048576"); 
		Job sample = Job.getInstance(confsample, "sample");
		sample.setJarByClass(pointrepart.class);
		sample.setMapperClass(SampleMapper.class);
		sample.setReducerClass(SampleReducer.class);
		sample.setMapOutputKeyClass(PointWritable.class);
		sample.setMapOutputValueClass(NullWritable.class);
		sample.setOutputKeyClass(PointWritable.class);
		sample.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(sample, new Path(args[7] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(sample, new Path(args[3]));
        ControlledJob ctrlsample = new ControlledJob(confsample);
        ctrlsample.setJob(sample);                
		
        
        
		Configuration confcluster = new Configuration();
		confcluster.set("centerfile", args[3]);
		confcluster.set("mapreduce.input.fileinputformat.split.maxsize", "262144");
		Job cluster = Job.getInstance(confcluster, "cluster");
		cluster.setJarByClass(pointrepart.class);
		cluster.setMapperClass(ClusterMapper.class);
		cluster.setReducerClass(ClusterReducer.class);
		cluster.setMapOutputKeyClass(PointWritable.class);
		cluster.setMapOutputValueClass(DoubleWritable.class);
		cluster.setOutputKeyClass(PointWritable.class);
		cluster.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(cluster, new Path(args[7] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(cluster, new Path(args[4]));
        ControlledJob ctrlcluster = new ControlledJob(confcluster);
        ctrlcluster.setJob(cluster);
        
        
        
        Configuration confrepartition = new Configuration();
        confrepartition.set("threshold", args[0]);
        confrepartition.set("samplerate", args[1]);
        confrepartition.set("centerfile", args[4]);
        confrepartition.set("mapreduce.input.fileinputformat.split.maxsize", "524288");
        Job repartition = Job.getInstance(confrepartition, "repartition");
        repartition.setNumReduceTasks(16);
        repartition.setJarByClass(pointrepart.class);
        repartition.setMapperClass(similarityMapper.class);
        repartition.setReducerClass(repartitionReducer.class);
        repartition.setMapOutputKeyClass(PointWritable.class);
        repartition.setMapOutputValueClass(PointWritable.class);
        repartition.setOutputKeyClass(PointWritable.class);
        repartition.setOutputValueClass(PointWritable.class);
        FileInputFormat.setInputPaths(repartition, new Path(args[7] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(repartition, new Path(args[5]));
        ControlledJob ctrlrepartition = new ControlledJob(confrepartition);
        ctrlrepartition.setJob(repartition);
    
        
        
        Configuration confsimilarity = new Configuration();
        confsimilarity.set("threshold", args[0]);
        confsimilarity.set("mapreduce.input.fileinputformat.split.maxsize", "268435456");
        Job similarity = Job.getInstance(confsimilarity, "similarity");
        similarity.setNumReduceTasks(20);
        similarity.setJarByClass(pointrepart.class);
        similarity.setMapperClass(repartitionMapper.class);
        similarity.setReducerClass(similarityReducer.class);
        similarity.setMapOutputKeyClass(PointWritable.class);
        similarity.setMapOutputValueClass(PointWritable.class);
        similarity.setOutputKeyClass(PairWritable.class);
		similarity.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(similarity, new Path(args[5]));
        FileOutputFormat.setOutputPath(similarity, new Path(args[6]));
        ControlledJob ctrlsimilarity = new ControlledJob(confsimilarity);
        ctrlsimilarity.setJob(similarity);
        
        
        
        
        // System.exit(similarity.waitForCompletion(true) ? 0 : 1);        
        
        
        
        ctrlsample.addDependingJob(ctrldatain);
        ctrlcluster.addDependingJob(ctrlsample);
        ctrlrepartition.addDependingJob(ctrlcluster);
        ctrlsimilarity.addDependingJob(ctrlrepartition);
        
        JobControl allcontroller = new JobControl("AllController");
        allcontroller.addJob(ctrldatain);
        allcontroller.addJob(ctrlsample);
        allcontroller.addJob(ctrlcluster);
        allcontroller.addJob(ctrlrepartition);
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
