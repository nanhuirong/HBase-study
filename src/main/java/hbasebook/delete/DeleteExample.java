package hbasebook.delete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nanhuirong on 16-7-22.
 */
public class DeleteExample {
    public final static String TEST_TABLE = "testtable";
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }



    public static void main(String[] args)throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);

        //单行Delete
//        Delete delete = new Delete(Bytes.toBytes("row11"));
//        //如果不指定版本号，删除全部的单元格；
//        //但是基于时间戳的删除必须在PUT时加入版本号，否则基于时间戳的删除没有意义
//        //因为时间戳是服务器给定的
//        delete.deleteColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
//        //删除列族的所有版本
//        delete.deleteFamily(Bytes.toBytes("colfam1"));
//        table.delete(delete);

        //Delete列表
        List<Delete> deletes = new ArrayList<Delete>();

        table.delete(deletes);



        table.close();

    }
}
