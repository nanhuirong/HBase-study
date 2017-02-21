package hbasebook.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-22.
 */
public class GetExample {
    public static final String TEST_TABLE = "testtable";
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }
    public static void main(String[] args)throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);
//        //单行GET, 只限定行数, 不限定列数
        Get get = new Get(Bytes.toBytes(("row11")));
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
        Result result = table.get(get);
        byte[] qual1 = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        byte[] qual2 = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
        //返回键值对的数量
        System.out.println(result.size());
        System.out.println(Bytes.toString(qual1));
        System.out.println(Bytes.toString(qual2));
        table.close();

        //Get列表, 一次请求获取多行数据
//        byte[] cf1 = Bytes.toBytes("colfam1");
//        byte[] qf1 = Bytes.toBytes("qual1");
//        byte[] qf2 = Bytes.toBytes("qual2");
//        byte[] row1 = Bytes.toBytes("row-11");
//        byte[] row2 = Bytes.toBytes("row-12");
//        List<Get> gets = new ArrayList<Get>();
//        Get get1 = new Get(row1);
//        get1.addColumn(cf1, qf1);
//        get1.addColumn(cf1, qf2);
//        gets.add(get1);
//
//        Get get2 = new Get(row2);
//        get2.addColumn(cf1, qf1);
//        get2.addColumn(cf1, qf2);
//        gets.add(get2);
//
//        Result[] results = table.get(gets);
//        for (Result result: results){
//            System.out.println(Bytes.toString(result.getRow()));
//            byte[] val = null;
//            if (result.containsColumn(cf1, qf1)){
//                val = result.getValue(cf1, qf1);
//                System.out.println(Bytes.toString(val));
//            }
//            if (result.containsColumn(cf1, qf2)){
//                val = result.getValue(cf1, qf2);
//                System.out.println(Bytes.toString(val));
//            }
//        }
//
//        for (Result result: results){
//            System.out.println(result);
//        }
        table.close();
//        使用特殊检索方式








    }

}
