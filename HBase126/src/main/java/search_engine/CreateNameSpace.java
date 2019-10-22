package search_engine;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import javax.xml.crypto.KeySelector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/11 16:21
 * 4
 */
public class CreateNameSpace{

    private static Connection connection =null;
    private static Admin admin =null ;
    private static Table table = null;
    ExecutorService pool = Executors.newFixedThreadPool(3);

    @Before
    public void testConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        System.out.println("连接开始");
        configuration.set("hbase.zookeeper.quorum","172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        //configuration.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(configuration,pool);
        System.out.println("连接成功"+connection);
        admin = connection.getAdmin();


    }

    @Test
    public void testPut() throws IOException {
        Table table_info = connection.getTable(TableName.valueOf("table_info"));
        Put put = new Put(Bytes.toBytes("hj_ctillnow:result"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("owner"),Bytes.toBytes("ctillnow"));
        table_info.put(put);

    }

    @Test
    public void createNameSpace() throws IOException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("hj_ctillnow");
        admin.createNamespace(builder.build());
        System.out.println("创建命名空间成功");
    }

    @Test
    public void scanTable() throws IOException {
        Get get = new Get(Bytes.toBytes("com.aliyun.bbs:http/read/157403.html"));
        Table table = connection.getTable(TableName.valueOf("hj_ctillnow:data"));
        Result result = table.get(get);

        Cell[] cells=result.rawCells();
        for(Cell c:cells){
            String row = Bytes.toString(c.getRowArray(),c.getRowOffset(),c.getRowLength());
            String family = Bytes.toString(c.getFamilyArray(),c.getFamilyOffset(),c.getFamilyLength());
            String columName = Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
            String value = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
            System.out.println(row+","+family+","+columName+":"+value);
            System.out.println("------------");
        }


        }
        @Test
        public void listTable() throws IOException {
            TableName[] tableNames = admin.listTableNamesByNamespace("hj_ctillnow");
            for(TableName tableName :tableNames)
            System.out.println(Bytes.toString( tableName.getName()));
        }

        @Test
        public void createTable() throws IOException {
            HTableDescriptor hTableDescriptor = new HTableDescriptor( TableName.valueOf("hj_ctillnow:data"));
            hTableDescriptor.addFamily( new HColumnDescriptor("f"));
            hTableDescriptor.addFamily( new HColumnDescriptor("p"));
            hTableDescriptor.addFamily( new HColumnDescriptor("il"));
            hTableDescriptor.addFamily( new HColumnDescriptor("ol"));
            hTableDescriptor.addFamily( new HColumnDescriptor("ilink"));
            hTableDescriptor.addFamily( new HColumnDescriptor("olink"));
            admin.createTable(hTableDescriptor);
        }
        @Test
        public void createTablePageRank() throws IOException {
            HTableDescriptor hTableDescriptor = new HTableDescriptor( TableName.valueOf("hj_ctillnow:pagerank"));
            hTableDescriptor.addFamily( new HColumnDescriptor("info"));
            hTableDescriptor.addFamily( new HColumnDescriptor("t"));

            admin.createTable(hTableDescriptor);
        }
        @Test
        public void createTableKeyWord() throws IOException {
            HTableDescriptor hTableDescriptor = new HTableDescriptor( TableName.valueOf("hj_ctillnow:keyword"));
            hTableDescriptor.addFamily( new HColumnDescriptor("p"));

            admin.createTable(hTableDescriptor);
        }
        @Test
        public void createTableResult() throws IOException {
            HTableDescriptor hTableDescriptor = new HTableDescriptor( TableName.valueOf("hj_ctillnow:result"));
            hTableDescriptor.addFamily( new HColumnDescriptor("page"));

            admin.createTable(hTableDescriptor);
        }

    @Test
    public void delTable() throws IOException {

        admin.disableTable(TableName.valueOf("hj_ctillnow:pagerank"));
        admin.truncateTable(TableName.valueOf("hj_ctillnow:pagerank"),true);
        //admin.deleteTable(TableName.valueOf("hj_ctillnow:result"));



    }
    @Test
    public void addTag() throws IOException {
        Table table = connection.getTable(TableName.valueOf("hj_ctillnow:pagerank"));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        List<Put> list = new ArrayList();
        for(Result r:scanner){
            byte[] row = r.getRow();
            Put put = new Put(row);
            put.addColumn(Bytes.toBytes("t"),Bytes.toBytes("tag"),Bytes.toBytes("p"));
            list.add(put);

        }
        table.put(list);


    }
    @Test
    public void addFamily() throws IOException {
        TableName tableName = TableName.valueOf("hj_ctillnow:keyword");
        admin.disableTable(tableName);
        HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor("t"));
        admin.modifyTable(tableName,descriptor);
        admin.enableTable(tableName);


    }

    @Test
    public void scanTable1() throws IOException {
        Table table = connection.getTable(TableName.valueOf("hj_ctillnow:pagerank"));
       // SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("st"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes(2));
        Scan scan = new Scan();
        /*
        scan.setFilter(singleColumnValueFilter);
         scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("bas"));
        scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("st"));
        scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t"));
        scan.addFamily(Bytes.toBytes("il"));
        scan.addFamily(Bytes.toBytes("ol"));*/

        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            Cell[] cells = rs.rawCells();
            for (Cell c : cells) {
                String key = Bytes.toString(c.getRowArray(),c.getRowOffset(),c.getRowLength());
                String family = Bytes.toString(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
                String colum = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                String value = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                System.out.println(key +" : "+family + "," + colum + "  :" + value);


            }

            System.out.println("------------");

        }
    }




    }




