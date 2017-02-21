package practice.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by nanhuirong on 16-10-5.
 */
public class RegionObserverExample extends BaseRegionObserver{
    public static final Log LOG = LogFactory.getLog(HRegion.class);
    public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)throws IOException{
        LOG.debug("Got preGet for row:" + Bytes.toStringBinary(get.getRow()));
        if (Bytes.equals(get.getRow(), FIXED_ROW)){
            Put put = new Put(get.getRow());
            put.addColumn(FIXED_ROW, FIXED_ROW, Bytes.toBytes(System.currentTimeMillis()));
            CellScanner scanner = put.cellScanner();
            scanner.advance();
            Cell cell = scanner.current();
            LOG.debug("Had a match, adding fake cell:\t" + cell);
            results.add(cell);
        }
    }

}
