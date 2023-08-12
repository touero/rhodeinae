package utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class HBaseutil {
    static Admin admin;
    static Connection conn;

    public HBaseutil(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.216.111");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void createTable (String mytablename,String[] familynames) throws IOException {
        TableName tableName = TableName.valueOf(mytablename);
        if (admin.tableExists(tableName)){
            System.out.println("table existed and will delete it now");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String col:familynames){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        admin.close();
        System.out.println("create table success");
    }

    public void dropTable (String mytablename) throws IOException {
        TableName tableName = TableName.valueOf(mytablename);
        if (admin.tableExists(tableName)){
            System.out.println("table existed and will delete it now");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("finish");
        }
    }

    public void tableExists (String mytablename) throws IOException {
        TableName tableName = TableName.valueOf(mytablename);
        if (admin.tableExists(tableName)){
            System.out.println("table existed");
        }
        else {
            System.out.println("table not existed");
        }
    }

    public void addRecord(String mytablename, String rowkey, String[] familysname, String[] values) throws IOException {
        TableName tableName = TableName.valueOf(mytablename);
        Table table = conn.getTable(tableName);
        for (int i=0;i< familysname.length;i++){
            Put put = new Put(rowkey.getBytes());
            String[] fsCols = familysname[i].split(":");
            if (fsCols.length==1){
                put.addColumn(fsCols[0].getBytes(),"".getBytes(),values[i].getBytes());
            }
            else {
                put.addColumn(fsCols[0].getBytes(),fsCols[1].getBytes(),values[i].getBytes());
            }
            table.put(put);
            table.close();
            System.out.println("add data success");
        }
    }

    public void delRowKey(String mytablename, String rowkey) throws IOException {
        TableName tableName = TableName.valueOf(mytablename);
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(rowkey.getBytes());
        table.delete(delete);
        table.close();
        System.out.println("delete data success");
    }

    public void deleteFamily (String mytablename, String col) throws IOException {
        TableName tableName = TableName.valueOf(mytablename);
        if (admin.tableExists(tableName)){
            admin.deleteColumn(tableName, col.getBytes());
            System.out.println("delete col success");
        }
        else {
            System.out.println("table not existed");
        }

    }


    public void searchData(String mytablename) throws IOException {
        Table table = conn.getTable(TableName.valueOf(mytablename));
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

    public void modifyData (String mytablename,String col) throws IOException {
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(mytablename));
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            String nameAsString = columnFamily.getNameAsString();
            if (col.equals(nameAsString)) {
                columnFamily.setTimeToLive(5);
            }
        }
        admin.modifyTable(TableName.valueOf(mytablename), tableDescriptor);
        System.out.println("modify finish");
    }

}


