package utils;

public enum HadoopInfo{
    IP("192.168.216.111"),
    PORT("2181");

    private final String value;

    HadoopInfo(String value) {
        this.value = value;
    }
    public String value(){
        return this.value;
    }
}
