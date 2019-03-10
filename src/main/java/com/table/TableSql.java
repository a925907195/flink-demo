package com.table;

import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Types;

public class TableSql {

    public static void main(String[] args) throws Exception{
        System.out.println("main begin!");
        String jobName = "jobname";
        System.out.println("job name is :" + jobName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("env success!");
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        System.out.println("table Env success!");

        DataStreamSource<OrderRecord> input = env.fromElements(
                new OrderRecord(1, 9,"action1",200),
                new OrderRecord(2, 9,"action2",200),
                new OrderRecord(3, 9,"action1",200),
                new OrderRecord(4, 9,"action1",200),
                new OrderRecord(5, 9,"action1",200),
                new OrderRecord(6, 9,"action1",200));
        System.out.println("input success!");
        tableEnv.registerDataStream("orders", input, "id, dealid, action, amount");
        // run a sql query on the Table and retrieve the result as a new Table
        Table table = tableEnv.sqlQuery(
                "SELECT id, dealid, action, amount FROM orders");
        System.out.println("table is : "+ table.toString());
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/search_data?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true")
                .setUsername("fjsh")
                .setPassword("271608")
                .setQuery("replace into orders (id, dealid, action, amount) values (?,?,?,?)")
                .setParameterTypes(new int[]{Types.BIGINT, Types.INTEGER, Types.VARCHAR, Types.DOUBLE})
                .build();
        System.out.println("jdbc Sink build success!");
        table.writeToSink(sink);
        System.out.println("flink all success!");
        env.execute("flinkDemo");
    }

    public static class OrderRecord {

        public long id;
        public int dealid;
        public String action;
        public double amount;

        public OrderRecord() {}

        public OrderRecord(long id, int dealid, String action, double amount) {
            this.id = id;
            this.dealid = dealid;
            this.action = action;
            this.amount = amount;
        }
    }
}
