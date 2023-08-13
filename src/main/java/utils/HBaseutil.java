package utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

import static org.joni.Config.log;

public class HBaseutil {
    static Admin admin;
    static Connection conn;
    static Configuration conf;

    public HBaseutil(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",HadoopInfo.IP.value());
        conf.set("hbase.zookeeper.property.clientPort",HadoopInfo.PORT.value());
    }

    public void getConnection() throws IOException{
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
            if (admin == null){
                log.println("in_OprHbase: [Connect Hbase Success]");
            }
        } catch (IOException e) {
            conn.close();
            log.println("in_OprHbase: [Close Connection]");
            e.printStackTrace();
        }
    }

    public void createTable (String connedTableName,String[] familyNames) throws IOException {
        TableName tableName = TableName.valueOf(connedTableName);
        if (admin.tableExists(tableName)){
            System.out.println("in_OprHbase: table existed and will delete it now");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String col: familyNames){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        admin.close();
        System.out.println("in_OprHbase: create table success");
    }

    public void dropTable (String connedTableName) throws IOException {
        TableName tableName = TableName.valueOf(connedTableName);
        if (admin.tableExists(tableName)){
            System.out.println("in_OprHbase: table existed and will delete it now");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("in_OprHbase: finish");
        }
    }

    public void tableExists (String connedTableName) throws IOException {
        TableName tableName = TableName.valueOf(connedTableName);
        if (admin.tableExists(tableName)){
            System.out.println("in_OprHbase: table existed");
        }
        else {
            System.out.println("in_OprHbase: table not existed");
        }
    }

    public void addRecord(String connedTableName, String rowKey, String[] familysname, String[] values) throws IOException {
        TableName tableName = TableName.valueOf(connedTableName);
        Table table = conn.getTable(tableName);
        for (int i=0;i< familysname.length;i++){
            Put put = new Put(rowKey.getBytes());
            String[] fsCols = familysname[i].split(":");
            if (fsCols.length==1){
                put.addColumn(fsCols[0].getBytes(),"".getBytes(),values[i].getBytes());
            }
            else {
                put.addColumn(fsCols[0].getBytes(),fsCols[1].getBytes(),values[i].getBytes());
            }
            table.put(put);
            table.close();
            System.out.println("in_OprHbase: add data success");
        }
    }

    public void delRowKey(String connedTableName, String rowKey) throws IOException {
        TableName tableName = TableName.valueOf(connedTableName);
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
        table.close();
        System.out.println("in_OprHbase: delete data success");
    }

    public void deleteFamily (String connedTableName, String col) throws IOException {
        TableName tableName = TableName.valueOf(connedTableName);
        if (admin.tableExists(tableName)){
            admin.deleteColumn(tableName, col.getBytes());
            System.out.println("in_OprHbase: delete col success");
        }
        else {
            System.out.println("in_OprHbase: table not existed");
        }
    }


    public void searchData(String connedTableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(connedTableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell c : cells) {
                byte[] rowArray = c.getRowArray();
                byte[] familyArray = c.getFamilyArray();
                byte[] qualifierArray = c.getQualifierArray();
                byte[] valueArray = c.getValueArray();
                System.out.print("行键:" + new String(rowArray, c.getRowOffset(), c.getRowLength()));
                System.out.print(" 列族:" + new String(familyArray, c.getFamilyOffset(), c.getFamilyLength()));
                System.out.print(" 列:" + new String(qualifierArray, c.getQualifierOffset(), c.getQualifierLength()));
                System.out.println(" 值:" + new String(valueArray, c.getValueOffset(), c.getValueLength()));
            }
        }
        table.close();
    }

    public void modifyData (String connedTableName,String col) throws IOException {
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(connedTableName));
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            String nameAsString = columnFamily.getNameAsString();
            if (col.equals(nameAsString)) {
                columnFamily.setTimeToLive(5);
            }
        }
        admin.modifyTable(TableName.valueOf(connedTableName), tableDescriptor);
        System.out.println("in_OprHbase: modify finish");
    }

}


