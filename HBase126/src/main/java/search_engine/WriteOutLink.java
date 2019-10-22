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
import org.apache.hadoop.io.DoubleWritable;
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
 * 3 * @Date: 2019/7/16 16:34
 * 4
 */
public class WriteOutLink  extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.set("hbase.zookeeper.quorum","computer1.cloud.briup.com:2181,computer2.cloud.briup.com:2181,computer3.cloud.briup.com:2181,computer4.cloud.briup.com:2181");
        Job job = Job.getInstance(configuration,"writeOutLink");
        job.setJarByClass(WriteOutLink.class);


        TableMapReduceUtil.initTableMapperJob(configuration.get("intable"),new Scan(), WriteOutLinkMapper.class,Text.class,Text.class,job);
        TableMapReduceUtil.initTableReducerJob(configuration.get("outtable"), WriteOutLintReducer.class,job);

        job.waitForCompletion(true);


        return 0;
    }
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new WriteOutLink(),args);
    }
    public static class WriteOutLinkMapper extends TableMapper<Text,Text>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            NavigableMap<byte[], byte[]> ol = value.getFamilyMap(Bytes.toBytes("ol"));
            if(ol.size()>0){
                byte[] k = value.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"));
                Set<Map.Entry<byte[], byte[]>> entries = ol.entrySet();
                StringBuffer stringBuffer = new StringBuffer();
                for(Map.Entry<byte[],byte[]> e:entries){
                    stringBuffer.append(Bytes.toString(e.getKey())).append(",");
                }
                context.write(new Text(k),new Text(stringBuffer.toString()));

            }


        }
    }
    public static class WriteOutLintReducer extends TableReducer<Text,Text,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer =new StringBuffer();
            for(Text t:values){
                stringBuffer.append(t.toString());
            }
            stringBuffer.substring(0,stringBuffer.length()-1);
            Put put = new Put(key.getBytes());
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("outlinks"),Bytes.toBytes(stringBuffer.toString()));
            context.write(NullWritable.get(),put);
        }
    }


}
