package utils;

public enum TaskType {
    VERSION("--version"),
    QUIT("--quit"),
    HELP("--help"),
    CREATETABLE("ct"),
    DROPTABLE("dt"),
    TABLEEXISTS("te"),
    ADDRECORD("ad"),
    DELROWKEY("dr"),
    DELETEFAMILY("df"),
    SEARCHDATA("sd"),
    MODIFYDATA("md");

    private final String value;

    TaskType(String value) {
        this.value = value;
    }
    public String value(){
        return this.value;
    }
}
