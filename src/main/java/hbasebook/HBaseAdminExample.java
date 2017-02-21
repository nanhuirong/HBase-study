package hbasebook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-24.
 */
public class HBaseAdminExample {
    public final static String TEST_TABLE = "testtable";
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    /**
     * 通过HBaseAdmin打印集群的信息
     * @throws IOException
     */
    public static void testHBaseAdmin()throws IOException{
        HBaseAdmin admin = new HBaseAdmin(conf);
        ClusterStatus status = admin.getClusterStatus();
        System.out.println("cluster status\n--------------------");
        System.out.println("Hbase版本\t" + status.getHBaseVersion());
        System.out.println("版本\t" + status.getVersion());
        System.out.println("在线Region\t" + status.getServersSize());
        System.out.println("集群ID\t" + status.getClusterId());
        System.out.println("集群Region\t" + status.getServers());
        System.out.println("死亡Region\t" + status.getDeadServers());
        System.out.println("Region个数\t" + status.getRegionsCount());
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();



        admin.close();
    }
    public static void main(String[] args)throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        System.out.println(admin.getClusterStatus());
        admin.close();
    }

}
