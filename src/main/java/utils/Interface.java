package utils;

import static org.joni.Config.log;
public class Interface {
    public Interface(){
        log.println("   ___                   _   _   _                          ");
        log.println("  / _ \\   _ __    _ __  | | | | | |__     __ _   ___    ___ ");
        log.println(" | | | | | '_ \\  | '__| | |_| | | '_ \\   / _` | / __|  / _ \\");
        log.println(" | |_| | | |_) | | |    |  _  | | |_) | | (_| | \\__ \\ |  __/");
        log.println("  \\___/  | .__/  |_|    |_| |_| |_.__/   \\__,_| |___/  \\___|");
        log.println("         |_|                                                 ");
        log.println("                                          --help: check command");
    }
    public void help(){
        log.println("   --version: check OprHbase version");
        log.println("   --quit: Quit");
        log.println("   --help: help");

        log.println("   The following data requires the name of the data table with a table");
        log.println("   example: [command] [connedTableName]");
        log.println("   ct: Create a table");
        log.println("   dt: Drop a table");
        log.println("   te: Check if this table exists");
        log.println("   ad: Add a record");
        log.println("   dr: Delete a record");
        log.println("   df: Delete a family");
        log.println("   sd: search data");
        log.println("   md: Up data data");

    }
    public void version(){
        log.println("oprHbase V1.0.0");
    }
}
