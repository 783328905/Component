package search_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
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
 * 3 * @Date: 2019/7/15 19:33
 *  表名 ->table_info行键 info owner 名字
 * 4
 */
public class FindAllKeyWord  extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new FindAllKeyWord(),args);
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.set("hbase.zookeeper.quorum","computer1.cloud.briup.com:2181,computer2.cloud.briup.com:2181,computer3.cloud.briup.com:2181,computer4.cloud.briup.com:2181");
        Job job = Job.getInstance(configuration,"keyword");
        job.setJarByClass(FindAllKeyWord.class);

        TableMapReduceUtil.initTableMapperJob(configuration.get("intable"),new Scan(),FindAllKeyWordMapper.class,Text.class,Text.class,job);
        TableMapReduceUtil.initTableReducerJob(configuration.get("outtable"),FindAllkeyWordReducer.class,job);
        job.waitForCompletion(true);



        return 0;
    }

    public static class FindAllKeyWordMapper extends TableMapper<Text,Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] bas = value.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"));

            NavigableMap<byte[], byte[]> map = value.getFamilyMap(Bytes.toBytes("il"));
            byte[] title = value.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"));
            if(map.size()>0){
                byte[] v = map.firstEntry().getValue();
                Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
                for(Map.Entry<byte[], byte[]> e:entries){
                    if(e.getValue().length!=0){
                        v=e.getValue();
                        break;
                    }

                }
                if(title!=null) {
                    context.write(new Text(bas),new Text(Bytes.toString(v)+"\t"+Bytes.toString(title)));

                }else {
                    context.write(new Text(bas), new Text(Bytes.toString(v)));
                }

            }


        }
    }

    public static class FindAllkeyWordReducer extends TableReducer<Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put =new Put(key.getBytes());
            for (Text v:values){
                put.addColumn(Bytes.toBytes("p"),Bytes.toBytes("keyword"),v.getBytes());

            }
            context.write(NullWritable.get(),put);
        }
    }

}
