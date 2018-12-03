package com.hadoop.mapreduce;

import com.jcraft.jsch.Buffer;
import com.sun.xml.bind.v2.runtime.reflect.Lister;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.io.*;
import java.lang.Math;

public class Likelihood {

    private static Map<String,String> mapc1 = new HashMap<>();
    private static Map<String,String> mapc2 = new HashMap<>();
    private static Map<String,Integer> map1 = new HashMap<>() ;
    private static int c0_count_all ;
    private static int c1_count_all ;
    private static int c0_cat_all ;
    private static int c1_cat_all;


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();


        @Override
        public void map(Object key,Text value ,Context context)
            throws IOException,InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            InputSplit inputSplit = context.getInputSplit();
            String FileName = ((FileSplit) inputSplit).getPath().getName();//得到split的txt文档名
            String DirName = ((FileSplit) inputSplit).getPath().getParent().getName();//得到split所属类
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                Text val = new Text(FileName);
                Text val1 = new Text(word+","+DirName);
                context.write(val, val1);//txt文档名  单词，txt文档类别
            }
        }

    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text>{

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)//values : 单词，文件类别
            throws IOException,InterruptedException {
            String v1,v2 ;
            double p0 = 0.0;
            double p1 = 0.0;
            double Ptkc0 ;
            double Ptkc1 ;
            String[] res0, res1;
            Text true_cat = new Text();
            for(Text val:values){//map函数结果的val【单词，文档所属类别】
                String doc_val = val.toString();
                String[] doc_value = doc_val.split(",");
                String k = doc_value[0]; //k是单词
                true_cat = new Text(doc_value[1]); //得到文档所属类别
                if(mapc1.containsKey(k)&&mapc2.containsKey(k)){ //如果k在mapc1和mapc2均能找到，则得到这个单词的p0和p1
                    v1 = mapc1.get(k);
                    v2 = mapc2.get(k);
                    res0 = v1.split(",");
                    res1 = v2.split(",");
                    p0 += Math.log(Double.parseDouble(res0[4]));
                    p1 += Math.log(Double.parseDouble(res1[4]));

                }
                else if(!mapc1.containsKey(k)&&mapc2.containsKey(k)){//k在mapc1找不到，在mapc2找到
                    p0 += Math.log((double)1/(c0_cat_all+c0_count_all));
                    v2 = mapc2.get(k);
                    res1 = v2.split(",");
                    p1 += Math.log(Double.parseDouble(res1[4]));
                }
                else if(mapc1.containsKey(k)&&!mapc2.containsKey(k)){//k在mapc1找到，在mapc2找不到
                    v1 = mapc1.get(k);
                    res0 = v1.split(",");
                    p0 += Math.log(Double.parseDouble(res0[4]));
                    p1 += Math.log((double)1/(c1_cat_all+c1_count_all));
                }
                else if(!mapc1.containsKey(k)&&!mapc2.containsKey(k)){//k在mapc1找不到，在mapc2也找不到
                    p0 += Math.log((double)1/(c0_cat_all+c0_count_all));
                    p1 += Math.log((double)1/(c1_cat_all+c1_count_all));
                }
            }
            Ptkc0 = p0+Math.log(0.5);
            Ptkc1 = p1+Math.log(160.0/325.0);
            if(Ptkc0>Ptkc1){
                Text result = new Text(true_cat+",I01001");
                context.write(key,result);
            }
            else if(Ptkc0<Ptkc1){
                Text result1 = new Text(true_cat+",I13000");
                context.write(key,result1);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        List<String>wordlist = new ArrayList<String>();
        int word_count = 0;
        int word_cat_count0 = 0;
        int word_cat_count1 = 0;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        Path inputpath = new Path("/output/part-r-00000"); //读取PrioriProbablity代码的运行输出结果
        Path outputpath = new Path("/result");
        Path outputpath1 = new Path("/result1");
        FSDataInputStream fsr,fsr1,fsDataInputStream;
        BufferedReader bufferedReader, bufferedReader0, bufferedReader1 ;
        String lineTxt, lineTxt0, lineTxt1 ;
        fsr = fileSystem.open(inputpath);
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        while ((lineTxt = bufferedReader.readLine()) != null){
            String[] res = lineTxt.split("\t");
            //lineTxt = lineTxt.replaceAll("\t",":");
            String[] res1 = res[1].split(",");
            if(res1[0].equals("I01001")&&!res1[1].equals("0")){//按行读取文件 若种类是I01001且单词个数大于0则单词种类数加1
                word_cat_count0 += 1;
                c0_count_all = Integer.parseInt(res1[2]); //I01001单词总数
            }
            if(res1[0].equals("I13000")&&!res1[1].equals("0")){//按行读取文件 若种类是I13000且单词个数大于0则单词种类数加1
                word_cat_count1 += 1;
                c1_count_all = Integer.parseInt(res1[2]); //I13000单词总数
            }
            //System.out.println(lineTxt);
        }
        c0_cat_all = word_cat_count0;
        c1_cat_all = word_cat_count1;
        fsr1 = fileSystem.open(inputpath);
        bufferedReader0 = new BufferedReader(new InputStreamReader(fsr1));
        while((lineTxt0 = bufferedReader0.readLine())!=null){
            String[] res2 = lineTxt0.split("\t");
            String[] res3 = res2[1].split(",");
            double p1,p2;
            //System.out.println(lineTxt0);
            if(res3[0].equals("I01001")&&!res3[1].equals("0")){
                p1 = (Double.parseDouble(res3[1])+1.0)/(Double.parseDouble(res3[2])+word_cat_count0);
                mapc1.put(res2[0],res2[1]+","+Integer.toString(word_cat_count0)+","+Double.toString(p1));
            }//mapc1存入[单词,{I01001，单词个数，这个类中的单词总数,这个类的单词种类数，去除zero问题后的P(t|c)}]
            if(res3[0].equals("I13000")&&!res3[1].equals("0")){
                p2 = (Double.parseDouble(res3[1])+1.0)/(Double.parseDouble(res3[2])+word_cat_count1);
                mapc2.put(res2[0],res2[1]+","+Integer.toString(word_cat_count1)+","+Double.toString(p2));
            }//mapc1存入[单词,{I13000，单词个数，这个类中的单词总数,这个类的单词种类数，去除zero问题后的P(t|c)}]
        }
        fsr.close();
        bufferedReader.close();
        fsr1.close();
        bufferedReader0.close();
        /*System.out.println(c0_cat_all+","+c0_count_all+"\n"+c1_cat_all+","+c1_count_all);
        for(Map.Entry<String,String>m:mapc1.entrySet()){
            String key = m.getKey();
            String value = m.getValue();
            System.out.println("key="+key+"  value="+value);
        }*/
        Path testpath = new Path("/Test/test.txt");
        fsr = fileSystem.open(testpath);
        String doc_path ;
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        while((lineTxt = bufferedReader.readLine())!=null){//统计文档路径 并存入map1（文档名，单词种类数）
            doc_path =lineTxt.substring(lineTxt.length()-30);
            wordlist.add(doc_path);
            //System.out.println(doc_path);
        }
        fsr.close();
        bufferedReader.close();

        Collections.shuffle(wordlist);//开始提交任务
        Job job = Job.getInstance(conf,"Likelihood");
        job.setJarByClass(Likelihood.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for(int i=0;i<wordlist.size()/2;i++) {
            FileInputFormat.addInputPath(job, new Path(wordlist.get(i)));
        }
        FileOutputFormat.setOutputPath(job,outputpath);
        //System.exit(job.waitForCompletion(true)?0:1);
        job.waitForCompletion(true);

        Job job1 = Job.getInstance(conf,"Likelihood1");
        job1.setJarByClass(Likelihood.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        for(int i=wordlist.size()/2;i<wordlist.size();i++) {
            FileInputFormat.addInputPath(job1, new Path(wordlist.get(i)));
        }
        FileOutputFormat.setOutputPath(job1,outputpath1);
        System.exit(job1.waitForCompletion(true)?0:1);
        //



        Path resultpath = new Path("/result/part-r-00000");
        Path resultpath1 = new Path("/result1/part-r-00000");
        fsr =fileSystem.open(resultpath);
        fsr1 =fileSystem.open(resultpath1);
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        bufferedReader1 = new BufferedReader(new InputStreamReader(fsr1));
        double TP=0,TN=0,FN=0,FP=0;
        double TP1=0,TN1=0,FN1=0,FP1=0;
        double Pres0=0,Recall0=0,F10=0;
        double Pres1=0,Recall1=0,F11=0;
        double MacroPres,MacroF1,MacroRecall;
        double MicroPres,MicroF1,MicroRecall;
        while((lineTxt = bufferedReader.readLine())!=null){
            String[] classif = lineTxt.split("\t");
            String[] jieguo = classif[1].split(",");
            if(jieguo[0].equals("I01001")&&jieguo[1].equals("I01001")){
                TP += 1.0;
            }
            else if (jieguo[0].equals("I01001")&&jieguo[1].equals("I13000")){
                FN += 1.0;
            }
            else if (jieguo[0].equals("I13000")&&jieguo[1].equals("I01001")){
                FP += 1.0;
            }
            else if (jieguo[0].equals("I13000")&&jieguo[1].equals("I13000")){
                TN += 1.0;
            }
        }
        while((lineTxt1 = bufferedReader1.readLine())!=null){
            //System.out.println(lineTxt1);
            String[] classif1 = lineTxt1.split("\t");
            String[] jieguo1 = classif1[1].split(",");
            if(jieguo1[0].equals("I13000")&&jieguo1[1].equals("I13000")){
                TP1 += 1.0;
            }
            else if (jieguo1[0].equals("I13000")&&jieguo1[1].equals("I01001")){
                FN1 += 1.0;
            }
            else if (jieguo1[0].equals("I01001")&&jieguo1[1].equals("I13000")){
                FP1 += 1.0;
            }
            else if (jieguo1[0].equals("I01001")&&jieguo1[1].equals("I01001")){
                TN1 += 1.0;
            }
        }
        System.out.println(TP1+"  "+TP1+"  "+FP1);
        Pres0 = TP/(TP+FP);
        Pres1 = TP1/(TP1+FP1);
        Recall0 = TP/(TP+FN);
        Recall1 = TP1/(TP1+FN1);
        F10 = 2*Pres0*Recall0/(Pres0+Recall0);
        F11 = 2*Pres1*Recall1/(Pres1+Recall1);
        MacroPres = (Pres0+Pres1)/2.0;
        MacroRecall = (Recall0+Recall1)/2.0;
        MacroF1 = (F10+F11)/2.0;
        MicroPres = (TP+TP1)/(TP+FP+TP1+FP1);
        MicroRecall = (TP+TP1)/(TP+FN+TP1+FN1);
        MicroF1 = 2*MicroPres*MicroRecall/(MicroPres+MicroRecall);
        //System.out.println("pres0="+pres0+"\n"+"pres1="+pres1+"\n"+"pres_all="+pre_all);
        System.out.println("Pres0="+Pres0+"  Recall0="+Recall0+"  F10="+F10+"\n"
                +"Pres1="+Pres1+"  Recall1="+Recall1
        +"  F11="+F11+"\n"+"MacroPres="+MacroPres+"  MacroRecall="+MacroRecall+"  MacroF1="+MacroF1+"\n"
        +"MicroPres="+MicroPres+"  MicroRecall="+MicroRecall+"  MicroF1="+MicroF1);
    }
}
