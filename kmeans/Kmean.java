package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.VectorWritable;
//import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;
import java.net.*;


public class Kmean {
	public static class Map
       extends Mapper<Object, Text, LongWritable, Text>{
       	//ArrayList<VectorWritable> centers;
       	double[][] center;
    	protected void setup(Context context) throws IOException, InterruptedException{
    		try{		
    					Configuration conf = context.getConfiguration();
	    				String pathIn = conf.get("clusterInPath");
                        Path pt=new Path(pathIn);
                        System.out.println("USING"+pathIn);
                        FileSystem fs = FileSystem.get(conf);
                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                        String line;
                        line=br.readLine();
                        
	                        int i=0,j;
	                        int CENTER_NUM = 10;
	                        int DIM = 58;
	                        this.center = new double[CENTER_NUM][DIM];
	                        //this.centers = new ArrayList<VectorWritable>();
	                        while (line != null){

	                                System.out.println(line);
	                                
	                                String[] node = line.split(" ");
	                                
	                                if(!node[0].equals("COST:")){
		                                for(j=0;j<DIM;j++){
		                                	center[i][j] = Double.parseDouble(node[j]);
		                                }
		                                i++;
		                            }
		                            
		                            line=br.readLine();
		                            
	                        }


                    	
                }catch(Exception e){
            }
	    	
	    }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

	    	String text = value.toString();
	    	String[] spl = text.split(" ");
	    	int i =0;
	    	double[] data = new double[spl.length];
	    	for(i=0;i<spl.length;i++){
	    		data[i] = Double.parseDouble(spl[i]);
	    	}
	    	
	    	int clusterID = 0;

	    	double dist=0;
	    	double min = Double.MAX_VALUE;
	    	//double temp_sum = 0.0f;
	    	int clusternum = 10;
	    	int DIMS = 58;
	    	for(i=0 ; i<clusternum; i++) {
	    		//temp_sum = 0.0f;
	    		dist=0;
	    		for(int j=0; j<DIMS; j++) {	    			
	    			dist += Math.pow((data[j] - center[i][j]),2);
	    			//dist += data[j] - center[i][j];	    			
	    			//temp_sum += dist * dist;
	    		}
	    		dist = Math.pow(dist,0.5);
	    		if(min >= dist) {
	    			min = dist;
	    			clusterID = i;
	    		}
	    	}
	    	context.write(new LongWritable(clusterID), new Text(String.valueOf(min) + "@" + text));
	    	context.write(new LongWritable(-1), new Text(String.valueOf(min*min)));
	    	

	    }
    }


    public static class Reduce extends Reducer<LongWritable, Text, NullWritable, Text>{

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int DIM = 58;
			//int clusternum = 10;
			double[] cluster = new double[DIM];
			String msg="";
			double sum = 0;
			if(key.get() != -1){
				int count = 0;
				for (Text val : values) {
					count++;
					String line = val.toString();
					String[] data0 = line.split("@");
					String[] data = data0[1].split(" ");
					//1@1.0@3
					for(int i=0;i<DIM;i++){
						cluster[i] += Double.parseDouble(data[i]);
					}
				}
				
				for(int i=0;i<DIM;i++){
						cluster[i] /= count;
						msg+=String.valueOf(cluster[i]);
						if(i!=DIM-1){
							msg += " ";
						}

				}
			}else{
				
				for (Text val : values) {
					String line = val.toString();
					sum += Double.parseDouble(line);
				}

			}
			if(key.get() != -1)
				context.write(NullWritable.get(),new Text(msg));
			else{
				context.write(NullWritable.get(), new Text("COST: "+String.valueOf(sum)));
			}

		}
			
			
	}
    

	public static void main(String[] args) throws Exception {


	    Configuration conf = new Configuration();
	    //conf.set("clusterInPath", "inputc1");
	    conf.set("clusterInPath", args[2]);
	    long startTime = System.currentTimeMillis();
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(Kmean.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]+"_0"));
	    job.waitForCompletion(true);

	    int iter = 20;
	    int  i = 1;
	    for(i=1;i<iter;i++){
	    	String x = args[1]+"_"+(i-1)+"/part-r-00000";
			conf.set("clusterInPath", x);
			Job job2 = new Job(conf, "QQQ");
			
			
			job2.setJarByClass(Kmean.class);
			job2.setMapperClass(Map.class);
			job2.setReducerClass(Reduce.class);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[0]));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_"+i));
			job2.waitForCompletion(true);
			
	    }



	    long endTime   = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			System.out.println(totalTime);
	    
	}


}