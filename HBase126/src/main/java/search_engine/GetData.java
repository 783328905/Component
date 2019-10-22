package search_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
 * 3 * @Date: 2019/7/15 10:59
 * com.aliyun.bbs:http/read/157403.html
 * 列族 列名 含义
 * f,  bas  url
 * il  url  入链
 * ol  url  出连
 * s    s   分数
 */
public class GetData extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        ToolRunner.run(new GetData(), args);

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.set("hbase.zookeeper.quorum", "computer1.cloud.briup.com:2181,computer2.cloud.briup.com:2181,computer3.cloud.briup.com:2181,computer4.cloud.briup.com:2181");
        Job job = Job.getInstance(configuration, "getData");
        job.setJarByClass(GetData.class);


        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("bas"));
        scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("st"));
        //scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes("cnt"));
        //scan.addColumn(Bytes.toBytes("p"),Bytes.toBytes("t"));
        scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"));
        scan.addFamily(Bytes.toBytes("il"));
        scan.addFamily(Bytes.toBytes("ol"));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("st"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes(2));
        singleColumnValueFilter.setFilterIfMissing(true);
        scan.setFilter(singleColumnValueFilter);

        TableMapReduceUtil.initTableMapperJob(configuration.get("intable"), scan, GetDataMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(configuration.get("outtable"), getDataReducer.class, job);
        job.waitForCompletion(true);


        return 0;
    }

    public static class GetDataMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            Cell[] cells = value.rawCells();
            int inlink = 0;
            int outlink = 0;
            for (Cell c : cells) {

                String family = Bytes.toString(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
                String value1 = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                String qualifier = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                if (family.equals( "il"))
                    inlink++;

                else if (family.equals("ol"))
                    outlink++;

                stringBuffer.append(family).append(",").append(qualifier).append(",").append(value1).append("\t");
            }
            stringBuffer.append("ilink" + "," + "count" + "," + inlink + "\t");
            stringBuffer.append("olink" + "," + "count" + "," + outlink + "\t");
            context.write(new Text(key.get()), new Text(stringBuffer.toString()));


        }
    }

    public static class getDataReducer extends TableReducer<Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //List<Put> puts = new ArrayList<>();
            for (Text t : values) {
                String map[] = t.toString().split("\t");
                Put put = new Put(key.getBytes());
                for (String s : map) {
                    String[] split = s.split(",");
                    if(split.length>2) {
                        put.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]), Bytes.toBytes(split[2]));
                    }else{
                        put.addColumn(Bytes.toBytes(split[0]), Bytes.toBytes(split[1]), Bytes.toBytes(""));
                    }
                }
                context.write(NullWritable.get(), put);

            }


        }
    }

}
