package hbasebook.put;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nanhuirong on 16-7-21.
 */
public class PutList {
    public final static String TEST_TABLE = "testtable";
    public  static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }
    public static void main(String[] args)throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);
        List<Put> putList = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("row11"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        putList.add(put1);

        Put put2 = new Put(Bytes.toBytes("row12"));
        put2.add(Bytes.toBytes("colfa"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        putList.add(put2);

        Put put3 = new Put(Bytes.toBytes("row13"));
        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        putList.add(put3);

        table.put(putList);
        table.close();
    }
}
