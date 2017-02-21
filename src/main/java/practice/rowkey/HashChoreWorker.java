package practice.rowkey;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by nanhuirong on 16-10-7.
 * 创建split计算器
 */
public class HashChoreWorker implements SplitKeysCalculator{
    //随机取数目
    private int baseRecord;
    //rowKey生成器
    private RowKeyGenerator rowKeyGenerator;
    //取样时, 由取样数目和Region数目相除
    private int splitKeysBase;
    //splitKey个数
    private int splitKeysNumber;
    //splitKey结果
    private byte[][] splitKeys;

    public HashChoreWorker(int baseRecord, int prepareRegions){
        this.baseRecord = baseRecord;
        rowKeyGenerator = new HashRowKeyGenerator();
        splitKeysNumber = prepareRegions - 1;
        splitKeysBase = baseRecord / prepareRegions;
    }
    @Override
    public byte[][] calcSplitKeys(){
        splitKeys = new byte[splitKeysNumber][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        //生成baseRecord个数的RowKey
        for (int i = 0; i < baseRecord; i++){
            rows.add(rowKeyGenerator.nextID());
        }
        int pointer = 0;
        Iterator<byte[]> rowKeyIterator = rows.iterator();
        int index = 0;
        while (rowKeyIterator.hasNext()){
            byte[] tempRow = rowKeyIterator.next();
            rowKeyIterator.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)){
                if (index < splitKeysNumber){
                    splitKeys[index] = tempRow;
                    index++;
                }
            }
            pointer++;
        }
        rows.clear();
        rows = null;
        return splitKeys;
    }
}
