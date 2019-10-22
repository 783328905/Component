package search_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/16 18:56
 *  url info:pagerank value
 *  url p:keyword value
 */
public class Join extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.set("hbase.zookeeper.quorum","computer1.cloud.briup.com:2181,computer2.cloud.briup.com:2181,computer3.cloud.briup.com:2181,computer4.cloud.briup.com:2181");
        Job job = Job.getInstance(configuration,"join");
        job.setJarByClass(Join.class);

        List<Scan> list = new ArrayList();
        Scan scan1= new Scan();
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes("hj_ctillnow:keyword"));
        Scan scan2= new Scan();
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes("hj_ctillnow:pagerank"));
        list.add(scan1);
        list.add(scan2);

        job.setInputFormatClass(MultiTableInputFormat.class);
        TableMapReduceUtil.initTableMapperJob(list,JoinMapper.class,Text.class,Text.class,job);
        TableMapReduceUtil.initTableReducerJob(configuration.get("outtable"),JoinReducer.class,job);
        job.waitForCompletion(true);

        return 0;
    }
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new Join(),args);
    }

    public static class JoinMapper extends TableMapper<Text,Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] value1 = value.getValue(Bytes.toBytes("t"), Bytes.toBytes("tag"));

            if(Bytes.toString( value1).equals("p")){
                 byte[] pagerank = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("pagerank"));
                 if( pagerank!=null)
                    context.write(new Text(key.get()),new Text("p,"+Bytes.toString(pagerank)));


            }else if(Bytes.toString(value1).equals("k")){
                byte[] keyword = value.getValue(Bytes.toBytes("p"), Bytes.toBytes("keyword"));

                context.write(new Text(key.get()),new Text("k,"+Bytes.toString( keyword)));

            }



        }
    }
    public static class JoinReducer extends TableReducer<Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyword=null;
            String pagerank=null;
            for(Text v:values) {
                String[] split = v.toString().split(",");
                if (split[0].equals("p"))
                    pagerank=split[1];
                if (split[0].equals("k"))
                    keyword=split[1];

            }

            if(keyword!=null&&pagerank!=null) {
                Put put = new Put(Bytes.toBytes(keyword));
                put.addColumn(Bytes.toBytes("page"), Bytes.toBytes(key.toString()),Bytes.toBytes(Double.parseDouble(pagerank)));
                context.write(NullWritable.get(),put);
            }
        }



    }
}
