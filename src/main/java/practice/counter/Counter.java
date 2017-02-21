package practice.counter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import practice.util.TableTool;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-10-5.
 * HBase计数器
 */
public class Counter {
    public static void main(String[] args) throws IOException{
        Counter counter = new Counter();
        //测试计数器自增
//        counter.increaseCounter("FILTER_TABLE");

        //测试多计数器
        counter.mulIncreaseCounter("FILTER_TABLE");
    }

    /**
     * 使用计数器自赠方法
     */
    public void increaseCounter(String tableName)throws IOException{
        TableTool tableTool = new TableTool(tableName);
        HTable table = tableTool.getTable();
        long cn1 = table.incrementColumnValue(Bytes.toBytes("row-01"), Bytes.toBytes("colfam1"), Bytes.toBytes("counter"), 1);
        //不预写日志
        long cn2 = table.incrementColumnValue(Bytes.toBytes("row-01"), Bytes.toBytes("colfam1"), Bytes.toBytes("counter"), 1, false);

        long current = table.incrementColumnValue(Bytes.toBytes("row-01"), Bytes.toBytes("colfam1"), Bytes.toBytes("counter"), 0);
        long cn3 = table.incrementColumnValue(Bytes.toBytes("row-01"), Bytes.toBytes("colfam1"), Bytes.toBytes("counter"), -1);

        System.out.println("cn1\t" + cn1);
        System.out.println("cn2\t" + cn2);
        System.out.println("current\t" + current);
        System.out.println("cn3\t" + cn3);


        table.close();
        tableTool.close();
    }

    /**
     * 多计数器
     */
    public void mulIncreaseCounter(String tableName)throws IOException{
        TableTool tableTool = new TableTool(tableName);
        HTable table = tableTool.getTable();

        Increment increment1 = new Increment(Bytes.toBytes("row-01"));
        increment1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("counter1"), 1);
        increment1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("counter2"), 1);
        increment1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("counter3"), 10);

        Result result = table.increment(increment1);
        for (KeyValue kv : result.raw()){
            System.out.println(kv + "\t" + Bytes.toString(kv.getKey()) + "\t" + Bytes.toLong(kv.getValue()));
        }



        table.close();
        tableTool.close();
    }




}
