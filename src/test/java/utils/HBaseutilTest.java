package utils;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;

import static org.joni.Config.log;

public class HBaseutilTest {
    HBaseutil hBaseutil = new HBaseutil();
    @Test
    public void testConnection() throws IOException {
        hBaseutil.getConnection();
    }


    @Test
    public void testCreateTable() throws IOException {
        hBaseutil.createTable("apihbase",new String[]{"info1","info2"});
    }


    @Test
    public void testDropTable() throws IOException {
        hBaseutil.dropTable("apihbase");
    }


    @Test
    public void testTableExists() throws IOException {
        hBaseutil.tableExists("apihbase");
    }


    @Test
    public void testAddRecord() throws IOException {
        hBaseutil.addRecord("apihbase","rk01",new String[]{"info1","info2"},new String[]{"lisi","22"});
    }


    @Test
    public void testDelRowKey() throws IOException {
        hBaseutil.delRowKey("apihbase","rk01");
    }


    @Test
    public void testDeleteFamily() throws IOException {
        hBaseutil.deleteFamily("apihbase","info1");
    }

    @Test
    public void testSearchData() throws IOException {
        hBaseutil.searchData("apihbase");
    }


    @Test
    public void testModifyData() throws IOException {
        hBaseutil.modifyData("apihbase","info2");
    }
}
