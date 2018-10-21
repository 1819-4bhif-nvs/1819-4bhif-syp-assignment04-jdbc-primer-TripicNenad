package at.htl.footballLeague;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static junit.framework.TestCase.fail;

public class LeagueTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    public static  Connection conn;

    @BeforeClass
    public static void initJdbc()
    {
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING,USER,PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n"+ e.getMessage()+"\n");
            System.exit(1);
        }
    }

    @AfterClass
    public static void teardownJdbc()
    {
        //Tabelle Bundesliga löschen
        try
        {
            conn.createStatement().execute("DROP TABLE Bundesliga");
            System.out.println("Tabelle Bundesliga gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle Bundesliga konnte nicht gelöscht werden: \n"+
            e.getMessage());
        }

        //Connection schließen
        try {
            if (conn != null && !conn.isClosed())
            {
                conn.close();
                System.out.println("Goodbye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void ddl()
    {
        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE Bundesliga ("+
                    "ID INT constraint teamId_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"+
                    "rank INT,"+
                    "teamname VARCHAR(255) NOT NULL,"+
                    "city VARCHAR(255) NOT NULL, "+
                    "points INT )";
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public  void insertTeamsIntoLeague()
    {
        //Daten einfügen
        int countInserts = 0;
        try
        {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (1,'FC BAYERN MUENCHEN','MUENCHEN',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (2,'BORUSSIA DORTMUND','DORTMUND',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (3,'WERDER BREMEN','BREMEN',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (4,'RB LEIPZIG','LEIPZIG',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (5,'HERTHA BSC','BERLIN',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (6,'BORUSSIA MOENCHENGLADBACH','GLADBACH',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (7,'EINTRACHT FRANKFURT','FRANKFURT',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (8,'TSG HOFFENHEIM','HOFFENHEIM',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (9,'FC AUGSBURG','AUGSBURG',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (10,'FSV MAINZ 05','MAINZ',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (11,'VFL WOLFSBURG','WOLFSBURG',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (12,'SC FREIBURG','FREIBURG',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (13,'BAYER LEVERKUSEN','LEVERKUSEN',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (14,'1. FC NUERNBERG','NUERNBERG',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (15,'HANNOVER 96','HANNOVER',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (16,'FC SCHALKE 04','GELSENKIRCHEN',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (17,'VFB STUTTGART','STUTTGART',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO Bundesliga (rank,teamname,city,points)" + " VALUES (18,'FORTUNA DUESSELDORF','DUESSELDORF',0)";
            countInserts += stmt.executeUpdate(sql);
            System.out.println("Es wurden "+countInserts+" Teams hinzugefuegt");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
