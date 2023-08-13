package utils;

import java.io.IOException;
import java.util.*;

import static org.joni.Config.log;

public class ControlMaster {
    HBaseutil hBaseutil = new HBaseutil();
    public ControlMaster() throws IOException {
        hBaseutil.getConnection();
        Interface show = new Interface();
        Scanner input = new Scanner(System.in);
        while (true) {
            log.print("in_OprHbase: ");
            String commandInput = input.next();
            String taskType = null;
            String[] commandSplit = null;
            String connedTableName = null;
            if (commandInput != null) {
                commandSplit = commandInput.split(" ");
                taskType = commandSplit[0].toString();
                if (commandSplit.length > 1) {
                    connedTableName = commandSplit[1].toString();
                }
            }
            if (Objects.equals(taskType, TaskType.HELP.value())) {
                show.help();
            }
            if (Objects.equals(taskType, TaskType.VERSION.value())) {
                show.version();
            }
            if (Objects.equals(taskType, TaskType.QUIT.value())) {
                log.println("in_OprHbase: quit OprHbase success");
                break;
            }
            if (Objects.equals(taskType, TaskType.CREATETABLE.value())) {
                log.println("in_OprHbase: please input clo");
                String[] infos = input.next().split(" ");
                List<String> list = new ArrayList<>(Arrays.asList(infos));
                String[] array = list.toArray(new String[0]);
                hBaseutil.createTable(connedTableName, array);
            }
            if (Objects.equals(taskType, TaskType.DROPTABLE.value())) {
                hBaseutil.dropTable(connedTableName);
            }
            if (Objects.equals(taskType, TaskType.TABLEEXISTS.value())) {
                hBaseutil.tableExists(connedTableName);
            }
            if (Objects.equals(taskType, TaskType.ADDRECORD.value())) {
                log.println("in_OprHbase: please input row key");
                String rowKey = input.next();
                log.println("in_OprHbase: please input clo");
                String[] infos = input.next().split(" ");
                List<String> list = new ArrayList<>(Arrays.asList(infos));
                String[] array = list.toArray(new String[0]);
                log.println("in_OprHbase: please input clo data");
                String[] data = input.next().split(" ");
                List<String> dataList = new ArrayList<>(Arrays.asList(data));
                String[] dataString = dataList.toArray(new String[0]);
                hBaseutil.addRecord(connedTableName, rowKey, array, dataString);
            }
            if (Objects.equals(taskType, TaskType.DELROWKEY.value())) {
                log.println("in_OprHbase: please input row key");
                String rowKey = input.next();
                hBaseutil.delRowKey(connedTableName, rowKey);
            }
            if (Objects.equals(taskType, TaskType.DELETEFAMILY.value())) {
                log.println("in_OprHbase: please input info");
                String info = input.next();
                hBaseutil.deleteFamily(connedTableName, info);
            }
            if (Objects.equals(taskType, TaskType.SEARCHDATA.value())) {
                hBaseutil.searchData(connedTableName);
            }
            if (Objects.equals(taskType, TaskType.MODIFYDATA.value())) {
                log.println("in_OprHbase: please input row key");
                String info = input.next();
                hBaseutil.modifyData(connedTableName, info);
            }
            else
                log.println("in_OprHbase: Not find command");
        }
    }
}
