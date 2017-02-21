package practice.rowkey;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.Random;

/**
 * Created by nanhuirong on 16-10-7.
 */
public class HashRowKeyGenerator implements RowKeyGenerator {
    private long currentID = 1;
    private long currentTime = System.currentTimeMillis();
    private Random random = new Random();
    @Override
    public byte[] nextID(){
        try {
            currentTime += random.nextInt(1000);
            byte[] lowT = Bytes.copy(Bytes.toBytes(currentTime), 4, 4);
            byte[] lowU = Bytes.copy(Bytes.toBytes(currentID), 4, 4);
            //将LowT和Low UMD5后取字符串的前8位, 即取前32个bit, 再与currentID拼成一个byte[]
            byte[] temp = MD5Hash.getMD5AsHex(Bytes.add(lowU, lowT)).substring(0, 8).getBytes();
//            System.out.println(temp.length);
            byte[] array =  Bytes.add(temp, Bytes.toBytes(currentID));
//            System.out.println(array.length);
            return array;
        }finally {
            currentID++;
        }

    }


}
