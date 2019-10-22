package search_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;


/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/18 9:37
 * 4
 */
public class PageRank extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.set("hbase.zookeeper.quorum","computer1.cloud.briup.com:2181,computer2.cloud.briup.com:2181,computer3.cloud.briup.com:2181,computer4.cloud.briup.com:2181");
        Job job = Job.getInstance( configuration,"pagerank");
        job.setJarByClass(PageRank.class);

        TableMapReduceUtil.initTableMapperJob("hj_ctillnow:data",new Scan(),PageRankMapper1.class,Text.class,Text.class,job);
        TableMapReduceUtil.initTableReducerJob("hj_ctillnow:pagerank", PageRankReducer1.class,job);
        job.waitForCompletion(true);



        return 0;



    }
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new PageRank(),args);
    }
    public static class PageRankMapper1 extends TableMapper<Text,Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] value1 = value.getValue(Bytes.toBytes("olink"), Bytes.toBytes("count"));
            int count = Integer.parseInt( Bytes.toString(value1));
            byte[] bas = value.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"));
            NavigableMap<byte[], byte[]> ol = value.getFamilyMap(Bytes.toBytes("ol"));
            if(count>0){
                Set<Map.Entry<byte[], byte[]>> entries = ol.entrySet();
                for(Map.Entry<byte[], byte[]> e:entries){
                    context.write(new Text(Bytes.toString(e.getKey())),new Text(Double.toString(10.0/count)));
                }
            }else {
                context.write(new Text(bas),new Text(0+""));
            }


        }
    }
    public static class PageRankReducer1 extends TableReducer<Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;

            for(Text d:values){
                sum += Double.parseDouble(d.toString());
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("pagerank"),Bytes.toBytes(Double.toString(sum*0.85+0.15)));
            context.write(NullWritable.get(),put);
        }
    }
}
