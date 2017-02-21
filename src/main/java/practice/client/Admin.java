package practice.client;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import practice.util.TableTool;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by nanhuirong on 16-9-29.
 * 客户端的管理功能
 */
public class Admin {
    public static void main(String[] args)throws IOException {
        Admin admin = new Admin();
        admin.getClusterInfo();
    }

    /**
     *客户端API建表, 指定表名和列族
     */
    public void createTable(String tableName, List<String> colfams)throws IOException{
        TableTool tableTool = new TableTool(tableName);

        HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());
        //添加表格描述
        HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes(tableTool.getTableName()));
        for (String colfam : colfams){
            //添加列族描述
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(colfam);
            //向表描述中添加列族描述
            tableDescriptor.addCoprocessor(colfam);
        }
        //建表
        admin.createTable(tableDescriptor);
        //测试表格是否可见
        System.out.println("测试表格是否可见:\t" + admin.isTableAvailable(Bytes.toBytes(tableName)));
        tableTool.close();
        admin.close();
        return;
    }

    /**
     * 客户端高级建表功能, 通过预分区进行建表
     */
    public void createTable(String tableName, List<String> colfams, boolean isPartition)throws IOException{
        if (!isPartition){
            createTable(tableName, colfams);
        }else {
            TableTool tableTool = new TableTool(tableName);
            HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());

            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (String colfam : colfams){
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(colfam);
                tableDescriptor.addCoprocessor(colfam);
            }
            //预分区建表, 建立10个Region, 从1到100平均分配(并非严格平均分配)
            //每一个Region包含[startKey, stopKey),
            //第一个Region的startKey和最后一个Region的stopKey为空字节
            admin.createTable(tableDescriptor, Bytes.toBytes(1L), Bytes.toBytes(100L), 10);
            tableTool.close();
            admin.close();
        }
    }

    /**
     * 打印表格的Region信息
     */
    public void printTableRegions(String tableName)throws IOException{
        TableTool tableTool = new TableTool(tableName);
        System.out.println("打印" + tableName + "的Rehions信息");
        HTable table = tableTool.getTable();
        Pair<byte[][], byte[][]> pair = table.getStartEndKeys();
        for (int i = 0; i < pair.getFirst().length; i++){
            byte[] startKey = pair.getFirst()[i];
            byte[] stopKey = pair.getSecond()[i];
            System.out.println((i + 1) + "\t" + "<" + Bytes.toString(startKey) + "," + Bytes.toString(stopKey) + ">");
        }
        System.out.println("************************************");
        System.out.println("************************************");
        System.out.println("************************************");
        System.out.println("************************************");
        table.close();
    }

    /**
     * 获取所有的表结构
     */
    public void getTables()throws IOException{
        TableTool tableTool = new TableTool();
        HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());
        HTableDescriptor[] tableDescriptors = admin.listTables();
        for (HTableDescriptor descriptor : tableDescriptors){
            System.out.println(descriptor);
        }
        tableTool.close();
        admin.close();
    }

    /**
     * 获取特定的表结构
     */
    public void getTableDescriptor(String tableName)throws IOException{
        TableTool tableTool = new TableTool();
        HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());
        HTableDescriptor descriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));
        System.out.println(descriptor);
        tableTool.close();
        admin.close();
    }

    /**
     * 删除表格
     */
    public void deleteTable(String tableName)throws IOException{
        TableTool tableTool = new TableTool();
        HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());
        boolean isExist = admin.isTableAvailable(tableName);
        if (isExist){
            //删除表格必须先将表格禁用,
            //HRegionServer会将先将内存中近期未提交的数据刷写到磁盘, 然后关闭Region, 更新表的元数据, 将所有的Region标记为下线状态
            //直接删除会报错
            while (true){
                admin.disableTable(Bytes.toBytes(tableName));
                admin.deleteTable(Bytes.toBytes(tableName));
                //即使表格禁用, 依然返回true
                isExist = admin.isTableAvailable(tableName);
                if (!isExist){
                    break;
                }
            }
            tableTool.close();
            admin.close();
        }
    }

    /**
     * 修改表结构
     */
    public void modifyTable(String tableName, List<String> addColfams)throws IOException{
        TableTool tableTool = new TableTool();
        HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));
        System.out.println("修改前");
        System.out.println(tableDescriptor);
        for (String colfam : addColfams){
            boolean isExist = tableDescriptor.hasFamily(Bytes.toBytes(colfam));
            if (!isExist){
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(colfam));
                tableDescriptor.addFamily(columnDescriptor);
            }
        }
        //禁用表格
        boolean isEnable;
        while (true){
            admin.disableTable(Bytes.toBytes(tableName));
            isEnable = admin.isTableDisabled(Bytes.toBytes(tableName));
            if (isEnable){
                admin.modifyTable(tableName, tableDescriptor);
                //表格重新启用
                admin.enableTable(tableName);
                System.out.println("表格是否可用" + admin.isTableAvailable(Bytes.toBytes(tableName)));
                break;
            }
        }
        tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));
        tableTool.close();
        admin.close();
    }

    /**
     * 集群管理API
     */
    public void getClusterInfo()throws IOException{
        TableTool tableTool = new TableTool();
        HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());
        ClusterStatus status = admin.getClusterStatus();
        System.out.println("集群状态信息:\n************************************");
        System.out.println("Hbase Version:\t" + status.getHBaseVersion());
        System.out.println("ClusterStatus实例的版本号:\t" + status.getVersion());
        System.out.println("活跃的HRegionServer服务器\t" + status.getServersSize());
        System.out.println("集群ID\t" + status.getClusterId());
        System.out.println("活跃的HRegionServer\t" + status.getServers());
        System.out.println("死亡的HRegionServer服务器\t" + status.getDeadServers());
        System.out.println("在线的Region数量\t" + status.getRegionsCount());
        System.out.println("正在处理Region事务(移动, 上线, 下线)\t" + status.getRegionsInTransition());
        System.out.println("集群请求的TPS\t" + status.getRequestsCount());
        System.out.println("平均每台HRegionServer上线了多少Region\t" + status.getAverageLoad());
        System.out.println("*************************************************************");
        System.out.println("*************************************************************");
        System.out.println("*************************************************************");
        System.out.println("*************************************************************");
        System.out.println("HRegionServer信息");
        for (ServerName serverName : status.getServers()){
            System.out.println("主机名\t" + serverName.getHostname());
            System.out.println("<host-name>:<rpc 端口>\t" + serverName.getHostAndPort());
            System.out.println("<hostname><rpc port><start-code>\t" + serverName.getServerName());
            System.out.println("RPC 端口\t" + serverName.getPort());
            System.out.println("服务器启动时间\t" + serverName.getStartcode());
            System.out.println("*************************************************************");
            System.out.println("*************************************************************");
            System.out.println("*************************************************************");
            System.out.println("*************************************************************");
            System.out.println("RegionInfo");
            ServerLoad load = status.getLoad(serverName);
            System.out.println("Region数量\t" + load.getLoad());
            System.out.println("堆内存最大值(MB)\t" + load.getMaxHeapMB());
            System.out.println("堆内存的使用值(MB)\t" + load.getUsedHeapMB());
            System.out.println("当前Region服务器写缓存总大小(MB)\t" + load.getMemstoreSizeInMB());
            System.out.println("当前服务器的Region数量\t" + load.getNumberOfRegions());
            System.out.println("当前Region服务器的请求数\t" + load.getReadRequestsCount());
            System.out.println("当前Region服务器文件的存储量(MB)\t" + load.getStorefileSizeInMB());
            System.out.println("当前Region服务器文件的索引大小(MB)\t" + load.getStorefileIndexSizeInMB());
            System.out.println("*************************************************************");
            System.out.println("*************************************************************");
            System.out.println("*************************************************************");
            System.out.println("*************************************************************");
            System.out.println("Region Load");
            for (Map.Entry<byte[], RegionLoad> entry : load.getRegionsLoad().entrySet()){
                System.out.println("Region\t" + Bytes.toString(entry.getKey()));
                RegionLoad regionLoad = entry.getValue();
                System.out.println("Region 名\t" + Bytes.toString(regionLoad.getName()));
                System.out.println("Region的列族数目\t" + regionLoad.getStores());
                System.out.println("Region存储文件数量\t" + regionLoad.getStorefiles());
                System.out.println("Region存储文件占用的空间(MB)\t" + regionLoad.getStorefileSizeMB());
                System.out.println("Region存储的索引大小(MB)\t" + regionLoad.getStorefileIndexSizeMB());
                System.out.println("Region使用的MemStore大小\t" + regionLoad.getMemStoreSizeMB());
                System.out.println("Region本次统计周期内的TPS\t" + regionLoad.getRequestsCount());
                System.out.println("Region本次统计周期内的QPS(读)\t" + regionLoad.getReadRequestsCount());
                System.out.println("Region本次统计周期内的WPS(写)\t" + regionLoad.getWriteRequestsCount());
                System.out.println("*************************************************************");
                System.out.println("*************************************************************");
                System.out.println("*************************************************************");
                System.out.println("*************************************************************");
            }//for
        }//for
        tableTool.close();
        admin.close();
    }//getClusterInfo



}
