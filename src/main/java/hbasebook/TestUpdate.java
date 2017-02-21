package hbasebook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by nanhuirong on 16-7-21.
 */
public class TestUpdate {
    //平均插入时间是12S
    public static void testHDFS() throws IOException{
        String hdfsPath = "hdfs://59.67.152.231:9000/nan/test";
        Path path = new Path(hdfsPath);
        Configuration conf = new Configuration();
        for (int count = 0; count < 10; count++){
            FileSystem hdfs = path.getFileSystem(conf);
            if (hdfs.exists(path)){
                hdfs.delete(path);
            }
            FSDataOutputStream stream = hdfs.create(path);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10000000; i++){
                byte[] data = new byte[12];
                stream.write(data);
            }
            stream.close();
            long end = System.currentTimeMillis();
            System.out.println("1千万条数据--12个字节------->HDFS测试写入时间\t" + (end - start) / 1000 + " S");
        }

    }

    public static void testHBase()throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
        HTable table = new HTable(conf, "testtable");
//        table.setAutoFlush(false);
//        System.out.println(table.getWriteBufferSize() / (1024 * 1024) + "\tMB");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++){
            byte[] data = new byte[12];
            Put put = new Put(Bytes.toBytes("row-" + i));
//            put.setWriteToWAL(false);
            put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), data);
            table.put(put);
        }
        long end = System.currentTimeMillis();
//        table.flushCommits();
        table.close();
        System.out.println("1千万条数据--12个字节------->HBASE测试写入时间\t" + (end - start) / 1000 + " S");

    }
    public static void testHBaseWithBuffer(int count)throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
        HTable table = new HTable(conf, "testtable");
        table.setAutoFlush(false);
//        System.out.println(table.getWriteBufferSize() / (1024 * 1024) + "\tMB");
        table.setWriteBufferSize(count * 1024 * 1024);
        System.out.println(table.getWriteBufferSize() / (1024 * 1024) + "\tMB");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++){
            byte[] data = new byte[12];
            Put put = new Put(Bytes.toBytes("row-" + i));
//            put.setWriteToWAL(false);
            put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), data);
            table.put(put);
            if ((i % 1000000) == 0){
                System.out.println(i);
            }
        }
//        long end = System.currentTimeMillis();
        table.flushCommits();
        table.close();
        long end = System.currentTimeMillis();
        System.out.println("1千万条数据--12个字节------->HBASE测试写入时间使用客户端默认缓存\t" + (end - start) / 1000 + " S");

    }

    public static void testHBaseWithArrayList(int flush)throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
        HTable table = new HTable(conf, "testtable");
        long start = System.currentTimeMillis();
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10000000; i++){
            byte[] data = new byte[12];
            puts.add(new Put(Bytes.toBytes("row-" + i)).add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), data));
            if (puts.size() == flush){
                table.put(puts);
                puts.clear();
                System.out.println(i + "\t" +  puts.size());
            }
        }
        if (puts.size() > 0){
            table.put(puts);
            puts.clear();
        }
        table.close();
        long end = System.currentTimeMillis();
        System.out.println("1千万条数据--12个字节------->HBASE测试写入时间ArrayList\t" + (end - start) / 1000 + " S");
    }

    public static void testHBaseWithLinkedList(int flush)throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
        HTable table = new HTable(conf, "testtable");
        long start = System.currentTimeMillis();
        List<Put> puts = new LinkedList<Put>();
        for (int i = 0; i < 10000000; i++){
            byte[] data = new byte[12];
            puts.add(new Put(Bytes.toBytes("row-" + i)).add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), data));
            if (puts.size() == flush){
                table.put(puts);
                puts.clear();
                System.out.println(i + "\t" +  puts.size());
            }
        }
        if (puts.size() > 0){
            table.put(puts);
            puts.clear();
        }
        table.close();
        long end = System.currentTimeMillis();
        System.out.println("1千万条数据--12个字节------->HBASE测试写入时间LinkedList\t" + (end - start) / 1000 + " S");
    }

    public static void testHBaseWithBufferWithWAL(int count)throws IOException{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
        HTable table = new HTable(conf, "testtable");
        table.setAutoFlush(false);
//        System.out.println(table.getWriteBufferSize() / (1024 * 1024) + "\tMB");
        table.setWriteBufferSize(count * 1024 * 1024);
        System.out.println(table.getWriteBufferSize() / (1024 * 1024) + "\tMB");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++){
            byte[] data = new byte[12];
            Put put = new Put(Bytes.toBytes("row-" + i));
            put.setWriteToWAL(false);
            put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), data);
            table.put(put);
            if ((i % 1000000) == 0){
                System.out.println(i);
            }
        }
//        long end = System.currentTimeMillis();
        table.flushCommits();
        table.close();
        long end = System.currentTimeMillis();
        System.out.println("1千万条数据--12个字节------->HBASE测试写入时间使用客户端默认缓存\t" + (end - start) / 1000 + " S");

    }




    public static void main(String[] args)throws IOException{
//        testHDFS();
//        testHBaseWithArrayList(100000);
//        testHBaseWithLinkedList(100000);
//        testHBaseWithBuffer(10);
//        testHBaseWithBuffer(20);
//        testHBaseWithBuffer(50);
//        testHBaseWithBuffer(100);
        testHBaseWithBufferWithWAL(2);
//        testHBase();
    }

}
