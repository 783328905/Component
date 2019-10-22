package com.briup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.http.client.CookieStore;



/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/11 10:01
 * 4
 */
public class HiveJDBCTest {


    public static final String URL= "jdbc:hive2://192.168.25.164:10000/bd1902";
    public static final String username = "hj_ctillnow";
    public static final String password = "hdfs";




    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection(URL, username, password);
        String sql= "create table t_student(id int,name string) row format delimited fields terminated by ',' stored as textfile";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
        System.out.println("建表成功");



    }
}
