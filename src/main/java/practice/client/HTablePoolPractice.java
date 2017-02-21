package practice.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import practice.util.TableTool;

import java.io.IOException;
import java.util.LinkedList;


/**
 * Created by nanhuirong on 16-10-5.
 * HTablePool为HBase集群提供客户端连接池
 */
public class HTablePoolPractice {
    public static void main(String[] args)throws IOException {
        TableTool tableTool = new TableTool();
        Configuration conf = tableTool.getConf();
        HTablePool pool = new HTablePool(conf, 5);
        LinkedList<HTableInterface> list = new LinkedList<HTableInterface>();
        for (int i = 0; i < 10; i++){
            list.add(pool.getTable("FILTER_TABLE"));
            System.out.println(Bytes.toString(list.get(i).getTableName()));
        }

        for (int i = 0; i < 5; i++){
            pool.putTable(list.getFirst());
        }
        pool.closeTablePool("FILTER_TABLE");
    }
}
