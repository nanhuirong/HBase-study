package hbasebook.put;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-21.
 */
public class PutExample {

    public final static String TEST_TABLE = "testtable";
    public  static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    public static void main(String[] args)throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);

        //单行PUT操作
//        Put put = new Put(Bytes.toBytes("row1"));
//        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
//        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
//        table.put(put);
        //客户端写缓冲区练习, 默认禁用
        System.out.println("客户端写缓冲区是否禁用\t" + table.isAutoFlush());
        //启用客户端写缓冲区, 默认缓冲区大小是2MB
        table.setAutoFlush(false);
        System.out.println("客户端写缓冲区是否禁用\t" + table.isAutoFlush());
        System.out.println(table.getWriteBufferSize()/(1024*1024) + "MB");
        table.setWriteBufferSize(1024*1024*4);
        System.out.println(table.getWriteBufferSize()/(1024*1024) + "MB");
        System.out.println("客户端写缓冲区是否禁用\t" + table.isAutoFlush());


//        Put put2 = new Put(Bytes.toBytes("row6"));
//        put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
//        put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
//        table.put(put2);
//
//        Put put3 = new Put(Bytes.toBytes("row7"));
//        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
//        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
//        table.put(put3);
//
//        Get get = new Get(Bytes.toBytes("row6"));
//        Result result = table.get(get);
//        System.out.println(get);
//        while (true){
//
//        }






        table.close();



    }


}
