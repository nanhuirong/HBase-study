package hbasebook.htablepool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-24.
 */
public class HTablePoolExample {
    public final static String TEST_TABLE = "testtable";
    public  static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    public static void testHTablePool()throws IOException{
        long start = System.currentTimeMillis();
        HTablePool pool = new HTablePool(conf, 5);
        HTableInterface[] tables = new HTableInterface[10];
        for (int i = 0; i < 10; i++){
            tables[i] = pool.getTable("testtable");
            System.out.println(Bytes.toString(tables[i].getTableName()));
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);

        for (int i = 0; i < 5; i++){
            pool.putTable(tables[i]);
        }

        pool.closeTablePool("testtable");
        end = System.currentTimeMillis();
        System.out.println(end - start);

    }

    /**
     * 测试Region的边界
     * @param args
     * @throws IOException
     */
    public static void testRegion()throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);
        Pair<byte[][], byte[][]> pair = table.getStartEndKeys();
        for (int i = 0; i < pair.getFirst().length; i++){
            byte[] startkey = pair.getFirst()[i];
            byte[] endkey = pair.getSecond()[i];
            System.out.println(Bytes.toStringBinary(startkey) + "|\t" + Bytes.toStringBinary(endkey));
        }
        table.close();
    }

    public static void main(String[] args)throws IOException{
//        testHTablePool();
        testRegion();
//        byte[] bytes = eb4ad5b627216996d5f168b1081a0531;
//        System.out.println(Bytes.toString(eb4ad5b627216996d5f168b1081a0531));
    }

}
