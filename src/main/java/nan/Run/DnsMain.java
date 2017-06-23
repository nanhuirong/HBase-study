package nan.Run;

import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.hbase.StorageManagement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by nanhuirong on 16-5-18.
 */
public class DnsMain {
    public static final String PATH = "/home/huirong/result/";
    public static final String RECORD = "netflow/";
    public static final String METRICS = "metrics/";
    public static final String SUFFIX = "part-00000";
    public static void main(String[] args)throws Exception {
        StorageManagement storageManagement = new StorageManagement();
        File file = new File(PATH + RECORD + SUFFIX);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = null;
        long count = 0;
        List<Netflow> list = new ArrayList<Netflow>();
//        while ((line = br.readLine()) != null) {
//            String[] split = line.split(",");
////            System.out.println(Arrays.toString(split));
//            Netflow netflow = new Netflow(split[1], split[2], split[3], split[4], split[5], split[6]);
//            list.add(netflow);
//            count++;
//            if (count % 100000 == 0) {
//                storageManagement.addApplicationNetflow(list);
//                list.clear();
//                System.out.println(count);
//            }
//        }
//        if (list.size() > 0){
//            storageManagement.addApplicationNetflow(list);
//            list.clear();
//        }
//        br.close();


        file = new File(PATH + METRICS + SUFFIX);
        br = new BufferedReader(new FileReader(file));
        while ((line = br.readLine()) != null){
            String[] splits = line.split(",");
            System.out.println(Arrays.toString(splits));
//            Metrics metrics = new Metrics(splits[0], splits[1], splits[2], splits[3]);
            Metrics metrics = new Metrics();
            metrics.setTime(splits[0]);
            metrics.setBytes(splits[2]);
            metrics.setLinks(splits[1]);
            metrics.setPackets(splits[3]);
            System.out.println(metrics);
            storageManagement.addApplicationMetrics(metrics);
        }
    }
}
