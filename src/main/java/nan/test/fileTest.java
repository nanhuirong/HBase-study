package nan.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by nanhuirong on 16-5-18.
 */
public class fileTest {
    public static final String PATH = "/home/nanhuirong/test/";
    public static final String RECORD = "dnsRecord/";
    public static final String METRICS = "dnsMetrics/";
    public static final String SUFFIX = "part-00000";
    public static void main(String[] args)throws Exception{
        File file = new File(PATH + RECORD + SUFFIX);
        BufferedReader br = new BufferedReader(new FileReader(file));
//        List<String> list = new ArrayList<String>();
        String line = null;
        long count = 0;
        while ((line = br.readLine()) != null){
            count++;
            System.out.println(line);
//            list.add(line);
        }
        br.close();
//        for (String l: list){
//            System.out.println(l);
//        }
        System.out.println(count);

    }
}
