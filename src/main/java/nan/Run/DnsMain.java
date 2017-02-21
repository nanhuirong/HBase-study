package nan.Run;

import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.hbase.StorageManagement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by nanhuirong on 16-5-18.
 */
public class DnsMain {
    public static final String PATH = "/home/yaoxin/test/";
    public static final String RECORD = "dnsRecord/";
    public static final String METRICS = "dnsMetrics/";
    public static final String SUFFIX = "part-00000";
    public static void main(String[] args)throws Exception{
        StorageManagement storageManagement = new StorageManagement();
        File file = new File(PATH + RECORD + SUFFIX);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = null;
//        long count = 0;
        while ((line = br.readLine()) != null){
            String[] split = line.split(",");
            Netflow netflow = new Netflow(split[0], split[1], split[2], split[3], split[4], split[5]);
            storageManagement.addApplicationNetflow(netflow);
//            System.out.println(line);
//            System.out.println(netflow.getTime() + "," + netflow.getSrcIp() + "," + netflow.getDstIp() + "," + netflow.getDstPort() + "," + netflow.getProtocol());
        }
        br.close();
        System.out.println("--------------------------netflow---------------------------------");
        file = new File(PATH + METRICS + SUFFIX);
        br = new BufferedReader(new FileReader(file));
        while ((line = br.readLine()) != null){
            String[] split = line.split(",");
            Metrics metrics = new Metrics(split[0], split[1], split[2], split[3]);
            storageManagement.addApplicationMetrics(metrics);
        }
        br.close();
        System.out.println("--------------------------metrics---------------------------------");
    }
}
