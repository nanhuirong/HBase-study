package hbasebook.scan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-22.
 */
public class ScanExample {
    public static final String TEST_TABLE = "testtable";
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    public static void main(String[] args)throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);
//        System.out.println(table.getScannerCaching());
        //针对全表扫描, 每一个next方法触发一次RPC调用
        Scan scan = new Scan();
        long start;
        long end;
        System.out.println(table.getTableDescriptor());
        start = System.currentTimeMillis();
        ResultScanner resultScanner = table.getScanner(scan);
//        for (Result result: resultScanner){
////            System.out.println(result);
//        }
        resultScanner.close();
        end = System.currentTimeMillis();
        System.out.println((end - start) / 1000 + "S");
        //让一次RPC请求获取多行数据,   由扫描器缓存实现
        //在表的层面打开, 表的所有扫描器的缓存都会生效
        //在扫描器层面打开, 只会影响当前的扫描器
        System.out.println(table.getScannerCaching());
    }

}
