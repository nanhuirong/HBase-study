package hbasebook.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-24.
 */
public class CounterExample {
    public static final String TEST_TABLE = "testtable";
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    public static void testCounter() throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);
        //默认WAL
        long cnt1 = table.incrementColumnValue(Bytes.toBytes("20115531"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), 1);
        System.out.println(cnt1);

//        Get get = new Get(Bytes.toBytes("20115531"));
////        get.addFamily(Bytes.toBytes("colfam1"));
//        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
//        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
//        Result result = table.get(get);
//        System.out.println(result);

        table.close();
    }

    //多计数器
    public static void testMulCounter()throws IOException{
        long start = System.currentTimeMillis();
        HTable table = new HTable(conf, TEST_TABLE);
//        Increment increment = new Increment(Bytes.toBytes("row20"));
//        increment.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), 1);
//        increment.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), 10);
//
//        Result result = table.increment(increment);
//        for (KeyValue kv: result.raw()){
//            System.out.println(kv + "\t" + Bytes.toLong(kv.getValue()));
//        }
        table.close();
        long end = System.currentTimeMillis();
        System.out.println((end - start));
        start = System.currentTimeMillis();
        int i = 0;
        end = System.currentTimeMillis();
        System.out.println((end - start));

    }
    public static void main(String[] args)throws IOException{
//        testCounter();
        testMulCounter();
    }

}
