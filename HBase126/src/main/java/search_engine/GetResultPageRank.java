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

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/16 17:03
 * 4  time 1
 */
public class GetResultPageRank extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.set("hbase.zookeeper.quorum", "computer1.cloud.briup.com:2181,computer2.cloud.briup.com:2181,computer3.cloud.briup.com:2181,computer4.cloud.briup.com:2181");
        Job job = Job.getInstance(configuration, "getResultPageRank");
        job.setJarByClass(GetResultPageRank.class);


        TableMapReduceUtil.initTableMapperJob("hj_ctillnow:pagerank", new Scan(), GetResultPageRankMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("hj_ctillnow:pagerank", GetResultPageRankReducer.class, job);

        job.waitForCompletion(true);


        return 0;
    }

    public static void main(String args[]) throws Exception {
        ToolRunner.run(new GetResultPageRank(), args);
    }

    public static class GetResultPageRankMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] value1 = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("pagerank"));
            byte[] value2 = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("outlinks"));
            if(value2!=null&&value1!=null) {
                String string = Bytes.toString(value2);
                String[] splits = string.split(",");
                if (splits.length > 0) {
                    for (String s : splits) {
                        context.write(new Text(s), new Text(Double.toString(Double.parseDouble(Bytes.toString(value1)) /(double) splits.length)));
                    }
                } else if (splits.length == 0) {
                    context.write(new Text(key.get()), new Text(0 + ""));

                }
            }else {
                context.write(new Text(Bytes.toString(key.get()).trim()), new Text(0 + ""));
            }
        }
    }


    public static class GetResultPageRankReducer extends TableReducer<Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (Text d : values) {
                sum += Double.parseDouble(d.toString());
            }

            Put put = new Put(key.getBytes());
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pagerank"), Bytes.toBytes(Double.toString(sum * 0.85 + 0.15)));
            context.write(NullWritable.get(), put);
        }
    }


}
