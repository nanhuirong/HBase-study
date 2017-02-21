package practice.rowkey;


import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Created by nanhuirong on 16-10-7.
 */
public class RowKeyTest {
    public static void main(String[] args)throws Exception {
        RowKeyTest test = new RowKeyTest();
        test.testHash();
        System.out.println("***********************************************************");
        System.out.println("***********************************************************");
        System.out.println("***********************************************************");
        System.out.println("***********************************************************");
        test.testPartition();
    }

    /**
     * 测试Hash
     */
    public void testHash()throws Exception{
        //100W个随机RowKey, 10个Region
        HashChoreWorker worker = new HashChoreWorker(1000000,10);
        byte[][] splitKeys = worker.calcSplitKeys();
        int row = splitKeys.length;
        int col = splitKeys[0].length;
        System.out.println(row);
        for (int i = 0; i < row; i++){
//            System.out.println(Bytes.toString(splitKeys[i]));
//            System.out.println(Bytes.toStringBinary(splitKeys[i]));
            System.out.println(splitKeys[i].length + "\t" + Arrays.toString(splitKeys[i]));
        }
    }

    /**
     * 测试Partition
     */
    public void testPartition()throws Exception{
        PartitionRowKeyManager manager = new PartitionRowKeyManager();
        //创建10个分区
        manager.setPartition(10);
        byte[][] splitKeys = manager.calcSplitKeys();
        int row = splitKeys.length;
        int col = splitKeys[0].length;
        System.out.println(row);
        for (int i = 0; i < row; i++){
//            System.out.println(Bytes.toString(splitKeys[i]));
//            System.out.println(Bytes.toStringBinary(splitKeys[i]));
            System.out.println(Arrays.toString(splitKeys[i]));
        }
    }
}
