package utils;

public enum TaskType {
    CREATETABLE(1),
    DROPTABLE(2),
    TABLEEXISTS(3),
    ADDRECORD(1),
    DELROWKEY(1),
    DELETEFAMILY(1),
    SEARCHDATA(1),
    MODIFYDATA(1);

    private final Integer value;

    TaskType(Integer value) {
        this.value = value;
    }
    public Integer value(){
        return this.value;
    }
}
