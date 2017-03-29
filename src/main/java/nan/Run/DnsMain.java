package nan.Run;

import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.hbase.StorageManagement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nanhuirong on 16-5-18.
 */
public class DnsMain {
    public static final String PATH = "/home/huirong/work/data/";
    public static final String RECORD = "dnsRecord/";
    public static final String METRICS = "dnsMetrics/";
    public static final String SUFFIX = "part-00000";
    public static void main(String[] args)throws Exception {
        StorageManagement storageManagement = new StorageManagement();
        File file = new File(PATH + "result.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = null;
        long count = 0;
        List<Netflow> list = new ArrayList<Netflow>();
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");
            Netflow netflow = new Netflow(split[0], split[1], split[2], split[3], split[4], split[5]);
            list.add(netflow);
            count++;
            if (count % 10000 == 0) {
                storageManagement.addApplicationNetflow(list);
                list.clear();
                System.out.println(count);
            }
        }
        if (list.size() > 0){
            storageManagement.addApplicationNetflow(list);
            list.clear();
        }
        br.close();
    }
}
