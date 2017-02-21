package hbasebook;

import practice.util.TableTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-22.
 */
public class HTableExample {
    public static void main(String[] args)throws IOException{
        TableTool tableTool = new TableTool("FILTER_TABLE");

        HTable table = tableTool.getTable();
        //获取表名
        System.out.println("表名" + Bytes.toString(table.getTableName()));
        Configuration conf =  table.getConfiguration();
        System.out.println("HTable实例的配置" + conf);
        //表示一个表结构
        HTableDescriptor descriptor = table.getTableDescriptor();
        System.out.println(descriptor);
        //检查表在Zookeeper中是否启用
//        System.out.println(HTable.isTableEnabled(table.getTableName()));
        //得到startKey, 按照Region进行切分
        byte[][] startKeys = table.getStartKeys();
        System.out.println("输出表的起始行健");
        for (int i = 0; i < startKeys.length; i++){
            System.out.println(Bytes.toString(startKeys[i]));
        }
        //得到Endkey
        byte[][] stopKeys = table.getEndKeys();
        System.out.println("输出表的终止行健");
        for (int i = 0; i < startKeys.length; i++){
            System.out.println(Bytes.toString(startKeys[i]));
        }
        System.out.println("输出表的起始, 终止行健");
        //得到起始, 终止行健对
        Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
        startKeys = startEndKeys.getFirst();
        stopKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++){
            System.out.println(Bytes.toString(startKeys[i]) + "\t" + Bytes.toString(stopKeys[i]));
        }

//        table.clearRegionCache();
        //得到Region信息
        HRegionLocation hRegionLocation = table.getRegionLocation("row-00");
        System.out.println("row-00的Region信息" + hRegionLocation);
        table.close();
    }
}
