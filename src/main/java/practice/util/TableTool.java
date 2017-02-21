package practice.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-9-24.
 * 设置获取表对象的工具类, 简化程序的编写
 */
public class TableTool {
    private String tableName;
    private HTable table;
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    public TableTool() {
    }

    public TableTool(String tableName) {
        this.tableName = tableName;
        setTable(tableName);
    }


    public void setTableName(String tableName){
        this.tableName = tableName;
    }

    public String getTableName(){
        return tableName;
    }

    public void setTable(String tableName){
//        setTableName(tableName);
        try {
            table = new HTable(conf, getTableName());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public HTable getTable(){
        return table;
    }

    public Configuration getConf(){
        return conf;
    }

    public void close()throws IOException{
        if (this.table != null){
            this.table.close();
        }
    }



}
