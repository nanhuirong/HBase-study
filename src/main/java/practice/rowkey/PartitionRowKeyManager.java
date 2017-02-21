package practice.rowkey;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by nanhuirong on 16-10-7.
 */
public class PartitionRowKeyManager implements RowKeyGenerator, SplitKeysCalculator {
    public static final int DEFAULT_PARTITION_AMOUNT = 20;
    private long currentID = 1;
    private int partition = DEFAULT_PARTITION_AMOUNT;

    public void setPartition(int partition){
        this.partition = partition;
    }

    @Override
    public byte[] nextID(){
        try {
            long partitionID = currentID % partition;
            return Bytes.add(Bytes.toBytes(partitionID), Bytes.toBytes(currentID));
        }finally {
            currentID++;
        }
    }
    @Override
    public byte[][] calcSplitKeys(){
        byte[][] splitKeys = new byte[partition - 1][];
        for (int i = 1; i < partition; i++){
            splitKeys[i - 1] = Bytes.toBytes((long)i);
        }
        return splitKeys;
    }
}
