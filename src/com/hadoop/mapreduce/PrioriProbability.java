package com.hadoop.mapreduce;

import com.jcraft.jsch.Buffer;
import com.sun.xml.bind.v2.runtime.reflect.Lister;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.io.*;

public class PrioriProbability {

    private static  int count0 = 0;
    private static  int count1 = 0;

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
             throws IOException,InterruptedException {
             StringTokenizer itr = new StringTokenizer(value.toString());
             InputSplit inputSplit = context.getInputSplit();
             String dirName = ((FileSplit) inputSplit).getPath().getParent().getName();
             while(itr.hasMoreTokens()){
                 word.set(itr.nextToken());
                 String one1 = one.toString();
                 if (dirName.equals("I01001")){
                     count0 += 1;
                 }
                 if (dirName.equals("I13000")){
                     count1 += 1;
                 }
                 Text val = new Text(dirName+","+one1);
                 context.write(word, val);
             }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text>{
        private Text result0 = new Text();
        private Text result1 = new Text();
        //private int count0 = 0;
        //private int count1 = 0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException,InterruptedException {
            int sum0 = 0;
            int sum1 = 0;
            String cat0 = "I01001";
            String cat1 = "I13000";
            for (Text val : values) {
                String val1 = val.toString();
                String[] res = val1.split(",");
                if (res[0].equals(cat0)) {
                    sum0 += Integer.parseInt(res[1]);
                }
                if (res[0].equals(cat1)) {
                    sum1 += Integer.parseInt(res[1]);
                }
            }
            result0.set(cat0+","+sum0+","+count0);
            result1.set(cat1+","+sum1+","+count1);
            context.write(key, result0);
            context.write(key, result1);
        }

    }

    /*private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly.");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }

    }*/

    private static ArrayList<Path> getFilesUnderFolder(FileSystem fs, Path folderPath, String pattern) throws IOException {
        ArrayList<Path> paths = new ArrayList<Path>();
        if (fs.exists(folderPath)) {
            FileStatus[] fileStatus = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatus.length; i++) {
                FileStatus fileStatu = fileStatus[i];
                if (!fileStatu.isDir()) {//只要文件
                    Path oneFilePath = fileStatu.getPath();
                    if (pattern == null) {
                        paths.add(oneFilePath);
                    } else {
                        if (oneFilePath.getName().contains(pattern)) {
                            paths.add(oneFilePath);
                        }
                    }
                }
            }
        }
        return paths;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path outputpath = new Path("/output");
        Path folderPath0 = new Path("/input/I01001");
        Path folderPath1 = new Path("/input/I13000");
        String pattern = null;
        ArrayList<Path> pathList0 = getFilesUnderFolder(fs, folderPath0, pattern);//select 90 next
        ArrayList<Path> pathList1 = getFilesUnderFolder(fs, folderPath1, pattern);//select 160 next
        Collections.shuffle(pathList0);
        Collections.shuffle(pathList1);
        int len0 = pathList0.size();
        int len1 = pathList1.size();
        Path filename = new Path("/Test/test.txt");
        FSDataOutputStream outStream = fs.create(filename);

        for(int i=90;i<180;i++){    //写入测试集文件目录
            String path0 = pathList0.get(i).toString()+"\n";
            outStream.writeUTF(path0);
        }
        for(int j=160;j<325;j++){
            String path1 = pathList1.get(j).toString()+"\n";
            outStream.writeUTF(path1);
        }
        outStream.close();

        //提交作业
        Job job = Job.getInstance(conf, "PrioriProbability");
        job.setJarByClass(PrioriProbability.class);
        job.setMapperClass(TokenizerMapper.class);
       // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for(int i=0;i<90;i++){
            FileInputFormat.addInputPath(job, pathList0.get(i));
        }
        for(int j=0;j<160;j++){
            FileInputFormat.addInputPath(job, pathList1.get(j));
        }
        FileOutputFormat.setOutputPath(job,outputpath);
        System.exit(job.waitForCompletion(true)?0:1);





    }


}
