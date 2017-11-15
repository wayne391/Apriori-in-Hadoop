package org.apache.hadoop.pagerank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank{
    private int N = 10876;
    public static class PhaseOneMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\n");

            for(int j=0; j<token.length; ++j){
                String[] val = token[j].split(",");

                if(val[0].equals("M")){
                    for (int i = 0; i < p; i++) {
                        context.write(new Text(val[1] + "," + i), new Text(val[0]+ "," + val[2] + "," + val[3]));
                    }
                }
                else if(val[0].equals("N")){
                    for (int i = 0; i < m; i++) {
                        context.write(new Text(i + "," + val[2]), new Text(val[0]+ "," + val[1] + "," + val[3]));
                    }
                }

            }
            
        }

    }
    public static class PhaseTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\n");

            for(int j=0; j<token.length; ++j){
                String[] val = token[j].split(",");

                if(val[0].equals("M")){
                    for (int i = 0; i < p; i++) {
                        context.write(new Text(val[1] + "," + i), new Text(val[0]+ "," + val[2] + "," + val[3]));
                    }
                }
                else if(val[0].equals("N")){
                    for (int i = 0; i < m; i++) {
                        context.write(new Text(i + "," + val[2]), new Text(val[0]+ "," + val[1] + "," + val[3]));
                    }
                }

            }
            
        }

    }



    public static class PhaseTwoReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, String> mapM = new HashMap<String, String>();
            Map<String, String> mapN = new HashMap<String, String>();

            for (Text value : values) {
                String[] val = value.toString().split(",");
                if ("M".equals(val[0])) {
                    mapM.put(val[1], val[2]);
                } 
                else{
                    mapN.put(val[1], val[2]);
                }
            }

            int result = 0;
            Iterator<String> mKeys = mapM.keySet().iterator();
            while (mKeys.hasNext()) {
                String mkey = mKeys.next();
                result += Integer.parseInt(mapM.get(mkey))
                        * Integer.parseInt(mapN.get(mkey));
            }
            context.write(new Text(key.toString()+ "," + result), NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        // set job configuration
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MatMul");
        job.setJarByClass(MatMul.class);

        for(int iter =0; i <20; i++){
            job.setMapperClass(PhaseOneMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(PhaseTwoMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            
            /
            job.setReducerClass(PhaseTwoReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

        }

        // set mapper's attributes
        
        // add file input/output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

 
}
