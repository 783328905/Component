package com.briup.base;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/3 11:08
 * 4
 */
public class ApiTest {

    private static Connection connection =null;
    private static Admin admin =null ;
    private static Table table = null;
    private static ExecutorService pool = Executors.newFixedThreadPool(3);

    public static void getConnection() throws IOException {
        //获取hbase配置对象
        Configuration configuration =  HBaseConfiguration.create();
        //增加配置项
        //写java代码
        System.out.println("连接开始");
        configuration.set("hbase.zookeeper.quorum","master:2181");
        //configuration.set("hbase.zookeeper.quorum", "192.168.25.164");
        //configuration.set("hbase.zookeeper.property.clientPort","2181");
        connection = ConnectionFactory.createConnection(configuration,pool);
        System.out.println("连接成功"+connection);
        //获得句柄对象
        admin =  connection.getAdmin();
        table =  connection.getTable(TableName.valueOf("bd1902:user"));





    }
    public static void getConnection_asyn() throws InterruptedException, ExecutionException, TimeoutException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","master:2181");
        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(configuration);
        System.out.println("操作1");
        System.out.println("操作2");
        System.out.println("操作3");
        AsyncConnection connection = asyncConnection.get(10000,TimeUnit.MILLISECONDS);
        System.out.println("异步连接获取成功"+connection);
        AsyncAdmin asyncAdmin = connection.getAdmin(pool);





    }
    public static  void createNS_asyn(String name) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(name);
        NamespaceDescriptor build = builder.build();
        Future<Void> namespaceAsync = admin.createNamespaceAsync(build);
        Void aVoid = namespaceAsync.get(10000, TimeUnit.SECONDS);
        System.out.println("异步创建成功!");



    }
    public static void close() throws IOException {
        connection.close();
        table.close();
        admin.close();
    }
    public static void createNS(String name) throws IOException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(name);
        NamespaceDescriptor build = builder.build();
        admin.createNamespace(build);
        System.out.println("同步创建成功");
    }
    public static void removeNameSpace(){

    }
    public static void createTable() throws IOException {

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("bd1902:cai"));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("name"));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("age"));
        columnFamilyDescriptorBuilder.setMaxVersions(5);
        columnFamilyDescriptorBuilder1.setMaxVersions(5);

        List<ColumnFamilyDescriptor> list = new ArrayList<>();
        list.add(columnFamilyDescriptorBuilder.build());
        list.add(columnFamilyDescriptorBuilder1.build());
        tableDescriptorBuilder.setColumnFamilies(list);
        byte[][] bytes = {Bytes.toBytes("1000"), Bytes.toBytes("2000"), Bytes.toBytes("3000")};

        Future<Void> tableAsync = admin.createTableAsync(tableDescriptorBuilder.build(), bytes);


    }

    //所有命名空间的所有表
    public static void listTables() throws IOException {
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName :tableNames)
            System.out.println(tableName);
    }
    public static void listTablesByNameSpace(Admin admin) throws IOException {
        TableName [] tableNames = admin.listTableNamesByNamespace("bd1902");
        for (TableName tableName:tableNames)
            System.out.println(tableName);

    }

    public static void scanTableBetween() throws IOException, DeserializationException {
        Table table = connection.getTable(TableName.valueOf("num"));

        Scan scan = new Scan();
        RowFilter rowFilter=new RowFilter(CompareOperator.GREATER,new BinaryComparator( Bytes.toBytes("1000")));
        scan.setFilter(rowFilter);
        ResultScanner scanner =table.getScanner(scan);
        for (Result rs:scanner){
            System.out.println("-----------------");
            System.out.println("rowkey"+Bytes.toString(rs.getRow()));
           
            Cell [] cells = rs.rawCells();
            for(Cell c:cells){
                String columnName = Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
                String columValue = null;
                if(columnName.equals("income")||columnName.equals("age")){
                    
                    int result = Bytes.toInt(c.getValueArray());
                    columValue =Integer.toString(result);

                }else {
                    columValue = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
                }
                System.out.println(columnName+":"+columValue);
            }
        }

    }
    public static void get(Table table) throws IOException {

        List<Get> gets= new ArrayList<>();
        gets.add(new Get(Bytes.toBytes(1000)));
        gets.add(new Get(Bytes.toBytes("1000")));
        //gets.add(new Get(Bytes.toBytes("rk003")));
        //gets.add(new Get(Bytes.toBytes("rk004")));
        Result[] results = table.get(gets);
        for(Result r : results ){
            Cell [] cells = r.rawCells();
            for (Cell c:cells){
                String columnName = Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
                String columnValue = null;
                if(columnName.equals("name")||columnName.equals("age")) {
                    int result = Bytes.toInt(c.getValueArray());
                    columnValue = Integer.toString(result);
                }else {
                    columnValue = Bytes.toString(c.getValueArray(),c.getValueOffset(),c.getValueLength());
                }
                System.out.println(columnName+":"+columnValue);
            }
        }


    }


    public static void delete(Table table) throws IOException {
        Delete delete = new Delete("name".getBytes());
        table.delete(delete);
        System.out.println("删除数据 name");

    }
    public static  void  add(Table table) throws IOException {
        Put put = new Put(Bytes.toBytes("1000"));
        put.addColumn(Bytes.toBytes("name"),Bytes.toBytes("tom"),Bytes.toBytes("sss"));
        put.addColumn(Bytes.toBytes("age"),Bytes.toBytes("tom"),Bytes.toBytes("19"));

        List<Put> puts = new ArrayList<>();
        puts.add(put);
        table.put(puts);



    }

    public static void selectWhere(Table table) throws IOException {
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        //过滤器 行键 列族 列名 值

        for (Result result:results){
            

        }

    }

    public static void main(String args[]) throws IOException, InterruptedException, ExecutionException, TimeoutException, DeserializationException {
        //getConnection_asyn();
        getConnection();
        //Table table = connection.getTable(TableName.valueOf(""));
        //add(table);
        //get(table);
        //createNS("briup");
        //createNS_asyn("ba");
        //listTablesByNameSpace(admin);
        scanTableBetween();
        //createTable();
        close();
    }


}
