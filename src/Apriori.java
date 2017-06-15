package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;

import java.util.*;
import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Apriori {
    public static boolean next_iter = true;
    
    public static class AprioriMapperIter extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private static ArrayList<ArrayList<String>> freq_item = new ArrayList<ArrayList<String>>();
        public static <T> ArrayList<String> union(ArrayList<String> list1,ArrayList<String> list2) {
            Set<T> set = new HashSet<T>();

            set.addAll((Collection<? extends T>) list1);
            set.addAll((Collection<? extends T>) list2);

            return (ArrayList<String>) new ArrayList<T>(set);
      }
     public static ArrayList<ArrayList<String>> carsetian_and_prune(){
         ArrayList<ArrayList<String>> new_freq_item = new ArrayList<ArrayList<String>>();
            
            
            for(int idx = 0;idx < freq_item.size();idx++){
                for(int jdx = idx + 1; jdx < freq_item.size(); jdx++){
                    
                    ArrayList<String> jlist = (ArrayList<String>) union(freq_item.get(idx), freq_item.get(jdx));
                    java.util.Collections.sort(jlist);
                    if(jlist.size() == freq_item.get(0).size() + 1){
                        new_freq_item.add(jlist);
                    }
                }
            }
            
            Set<ArrayList<String>> uniqueLists = new HashSet<>();
            uniqueLists.addAll(new_freq_item);
            new_freq_item.clear();
            new_freq_item.addAll(uniqueLists);
            ArrayList<ArrayList<String>> final_output = new ArrayList<ArrayList<String>>();
            for(int itemi = 0;itemi  < new_freq_item.size(); itemi++){
                int all_subset = 0;
                ArrayList<String> tmpL = new_freq_item.get(itemi);
                
                for(int ridx = 0; ridx <tmpL.size(); ridx++){
                    ArrayList<String> tmpSubset = new ArrayList(tmpL);          
                    tmpSubset.remove(ridx);
                    if(freq_item.contains(tmpSubset)){
                        all_subset += 1;
                    }
                }
                if(all_subset == tmpL.size()){
                    final_output.add(tmpL);
                } 
                
            }

            return final_output;         
     }
        protected void setup(Context context) throws IOException, InterruptedException{
            try{        
                    Configuration conf = context.getConfiguration();
                    String pathIn = conf.get("itemsetPath");
                    Path pt =new Path(pathIn);
  
                    FileSystem fs = FileSystem.get(conf);
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                    freq_item.clear();
                    String line;
                    line=br.readLine();

                    while (line != null){
                        String[] val = line.split(",");
                        ArrayList<String> sigle_freq_item = new ArrayList<>();
                        for (String tmps : val) {
                            sigle_freq_item.add(tmps);
                        }
                        java.util.Collections.sort(sigle_freq_item);
                        freq_item.add(sigle_freq_item); 

                        line=br.readLine();
                                    
                    }
                    
                    ArrayList<ArrayList<String>> new_freq_item =  carsetian_and_prune();
                    freq_item = new_freq_item;
                    
                    
                }catch(Exception e){
            }
            
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\n");

            for(int j=0; j<token.length; ++j){
                String[] val = token[j].split(",");
                ArrayList<String> new_item = new ArrayList<>();
                for (String tmps : val) {
                    new_item.add(tmps);   
                }

                for (int i = 0; i < freq_item.size(); i++){
                    ArrayList<String> tmpL = freq_item.get(i);
                    if(is_have_freq_itemset(tmpL, new_item)){
                        String key_itemset = "";
                        for (String s : tmpL)
                        {
                            key_itemset += s + ",";
                        }
                        context.write(new Text(key_itemset), one);
                    }
                }

            }
            
        }
    }

    public static class AprioriMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private static ArrayList<ArrayList<String>> freq_item = new ArrayList<ArrayList<String>>();

        protected void setup(Context context) throws IOException, InterruptedException{
            try{        
                    Configuration conf = context.getConfiguration();
                    String pathIn = conf.get("itemsetPath");
                    Path pt =new Path(pathIn);
  
                    FileSystem fs = FileSystem.get(conf);
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                    freq_item.clear();
                    String line;
                    line=br.readLine();
                    
                    while (line != null){
                        String[] val = line.split(",");
                        ArrayList<String> sigle_freq_item = new ArrayList<>();
                        for (String tmps : val) {
                            sigle_freq_item.add(tmps);
                        }
                        java.util.Collections.sort(sigle_freq_item);
                        freq_item.add(sigle_freq_item); 
                                
                                
                        line=br.readLine();
                                    
                    }
               
                }catch(Exception e){
            }
            
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\n");

            for(int j=0; j<token.length; ++j){
                String[] val = token[j].split(",");
                ArrayList<String> new_item = new ArrayList<>();
                for (String tmps : val) {
                    new_item.add(tmps);   
                }

                for (int i = 0; i < freq_item.size(); i++){
                    ArrayList<String> tmpL = freq_item.get(i);
                    if(is_have_freq_itemset(tmpL, new_item)){
                        String key_itemset = "";
                        for (String s : tmpL)
                        {
                            key_itemset += s + ",";
                        }
                        context.write(new Text(key_itemset), one);
                    }
                }

            }
            
        }
    }


    public static class AprioriReducer extends Reducer<Text,IntWritable,NullWritable,Text> {
        private IntWritable result = new IntWritable();
        private static int minSupport ;
        protected void setup(Context context) throws IOException, InterruptedException{
            try{        
                    Configuration conf = context.getConfiguration();
                    String str= conf.get("minSupport");
                    minSupport = Integer.valueOf(str);
                }catch(Exception e){
            }
            
        }
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(sum >= minSupport){
                result.set(sum);
                context.write(NullWritable.get(), key);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        // set job configuration
        Configuration conf = new Configuration();
        conf.set("itemsetPath", args[2]);
        conf.set("minSupport", args[3]);

        Job job = new Job(conf, "Apriori");
        job.setJarByClass(Apriori.class);

        // set mapper's attributes
        job.setMapperClass(AprioriMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        

        // set reducer's attributes
        job.setReducerClass(AprioriReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // add file input/output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"_0"));
        job.waitForCompletion(true);

        //----------- START ITERATION


        for(int i=1;i< Integer.valueOf(args[4]);i++){
        // int i = 1;
        // while(next_iter){

            String x = args[1]+"_"+(i-1)+"/part-r-00000";
            conf.set("itemsetPath", x);
            Job job2 = new Job(conf, "AprioriIter");
            job2.setJarByClass(Apriori.class);

            // set mapper's attributes
            job2.setMapperClass(AprioriMapperIter.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);
            

            // set reducer's attributes
            job2.setReducerClass(AprioriReducer.class);
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_"+i));
            job2.waitForCompletion(true);
            // i++;
            // if(next_iter == false) break;
        }
        //------------------------------------------------
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




    private static  boolean is_have_freq_itemset(ArrayList<String> tmpL,ArrayList<String> new_item){
        boolean has_subset = false;
        int all_same = 0;

        System.out.println(tmpL);
        for(String timpi :new_item){
            if(tmpL.contains(timpi)){
                all_same += 1;
            }
            
        }
        if(all_same == tmpL.size()){
            has_subset = true;
        }
        
        return has_subset;
    }
     

}