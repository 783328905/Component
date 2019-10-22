package optTable;


import com.sun.tracing.dtrace.ArgsAttributes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/1 16:28
 * 4
 */
public class HBaseTest {
    Configuration configuration =null;
    Connection connectionn = null;
    Admin admin = null;
    Pattern pattern =  Pattern.compile(".*user.*");
    @Before
    public void before() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.configuration.set("hbase.zookeeper.quorum","master:2181");
        this.connectionn = ConnectionFactory.createConnection(configuration);
        this.admin = connectionn.getAdmin();

    }
    @Test
    public void testCreate() throws Exception{


        //维护表信息
        if(!admin.tableExists(TableName.valueOf("test"))){
            TableName tableName =  TableName.valueOf("test");
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("name".getBytes());
            ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            admin.createTable(tableDescriptor);
            System.out.println("创建成功");


        }else {
            System.out.println("表已存在");
        }


    }
    @Test
    public void testListTables() throws IOException {

        TableName[] tableNames = admin.listTableNames(pattern);
        for (TableName tableName : tableNames){
            System.out.println(new String (tableName.getName()));
        }
    }
    @Test
    public void testListTablesByNameSpace() throws IOException {
        TableName [] tableNames = admin.listTableNamesByNamespace("default");

        for (TableName tableName : tableNames){
            System.out.println(new String (tableName.getName()));
        }
    }

    @Test
    public void CreateNameSpace() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("sss");
        Future<Void> namespaceAsync = admin.createNamespaceAsync(builder.build());
        Void aVoid = namespaceAsync.get(10000, TimeUnit.MILLISECONDS);
        System.out.println("异步创建成功");

    }
    @Test
    public void testCreateTable() throws IOException, InterruptedException, ExecutionException, TimeoutException {


        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("hello"));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info1"));

        List<ColumnFamilyDescriptor> list = new ArrayList<>();
        list.add(columnFamilyDescriptorBuilder.build());
        list.add(columnFamilyDescriptorBuilder1.build());
        builder.setColumnFamilies(list);
        byte[][] bytes = {Bytes.toBytes("1000"), Bytes.toBytes("2000"), Bytes.toBytes("3000")};


        Future<Void> tableAsync = admin.createTableAsync(builder.build(),bytes);
        tableAsync.get(10000,TimeUnit.MILLISECONDS);
        System.out.println("异步创建成功");



    }

    @Test
    public void delTable() throws IOException {

        admin.disableTable(TableName.valueOf("bd1902:cai"));
        admin.deleteTable( TableName.valueOf("bd1902:cai"));
    }

    @Test
    public void put() throws IOException {
        Table table = connectionn.getTable(TableName.valueOf("hello"));
        Put put = new Put(Bytes.toBytes("100"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("cai"));
        put.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("age"),Bytes.toBytes("10"));

        Put put1 = new Put(Bytes.toBytes("1100"));
        put1.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("age"),Bytes.toBytes("30"));
        List<Put> list = new ArrayList();
        list.add(put);
        list.add(put1);
        table.put(list);

    }

    @Test
    public void delete() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("100"));
        Delete delete1 = new Delete(Bytes.toBytes("102"));
        Table table = connectionn.getTable(TableName.valueOf("hello"));
        List<Delete> list = new ArrayList();
        list.add(delete);
        list.add(delete1);
        table.delete(list);


    }

    @Test
    public void scan() throws IOException {
        Get get = new Get(Bytes.toBytes("com.aliyun.bbs:http/read/157403.html"));
//        Get get1 = new Get(Bytes.toBytes("1100"));
//        List<Get>  list = new ArrayList<>();
//        list.add(get);
//        list.add(get1);
        Table table = connectionn.getTable(TableName.valueOf("hello"));
        Result result = table.get(get);

            Cell[] cells=result.rawCells();
            for(Cell c:cells){
                String row = Bytes.toString(c.getRowArray(),c.getRowOffset(),c.getRowLength());
                String columName = Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
                String value = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
                System.out.println(row+","+columName+":"+value);
                System.out.println("------------");
            }





    }

   /* @Test
    public void testVersionGet() throws IOException {
        Table table = connectionn.getTable(TableName.valueOf("hello"));
        Get q= new Get(Bytes.toBytes("100"));

        q.setMaxVersions(3);
        Result row= table.get(q);
        NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> allVersions=row.getMap();
        for(Map.Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> entry:allVersions.entrySet()){
            System.out.println(entry.getKey());
            NavigableMap<byte[], NavigableMap<Long, byte[]>> value =entry.getValue();
            for(Map.Entry entry1:(Map)value)

        }
        System.out.println(allVersions.toString());
    }*/
    @Test
    public void testVersionGet() throws IOException {
        Table table = connectionn.getTable(TableName.valueOf("bd1902:user"));
        Get get= new Get(Bytes.toBytes("1"));
        Get get1= new Get(Bytes.toBytes("1100"));
        get.setMaxVersions(3);
        get1.setMaxVersions(3);
        List<Get> list = new ArrayList();
        list.add(get);
        list.add(get1);
        

        Result[] results = table.get(list);
        for(Result result:results){
            for(Cell c:result.rawCells()){
                String row= Bytes.toString(c.getRowArray(),c.getRowOffset(),c.getRowLength());
                String cloum= Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
                String value= Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
                String family= Bytes.toString(c.getFamilyArray(),c.getFamilyOffset(),c.getFamilyLength());
                long timestamp = c.getTimestamp();
                System.out.println(row+","+family+","+cloum+","+value+","+timestamp);

            }
        }
    }

    @Test
    public void scanTable() throws IOException, DeserializationException {
        Table table = connectionn.getTable(TableName.valueOf("ns_telecom:calllog"));
        Scan scan = new Scan();
        //FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL, new RegexStringComparator("^info"));
        //RowFilter rowFilter = new RowFilter(CompareOperator.GREATER,new  BinaryComparator(Bytes.toBytes("99")));   //字典序比较
        //scan.setFilter(familyFilter);

        ResultScanner scanner = table.getScanner(scan);
        for(Result rs:scanner ){
            Cell[] cells = rs.rawCells();
            for(Cell c:cells){
                String row = Bytes.toString(c.getRowArray(),c.getRowOffset(),c.getRowLength());
                String colum = Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
                String family = Bytes.toString(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
                String value = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
                System.out.println(row+","+family+"--"+colum+":"+value);


            }
            System.out.println("------------");
        }


    }


}
