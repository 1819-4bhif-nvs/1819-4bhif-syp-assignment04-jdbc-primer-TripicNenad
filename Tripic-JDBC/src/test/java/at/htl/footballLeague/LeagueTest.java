package at.htl.footballLeague;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;


import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

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

        //Tabelle Premier League löschen
        try
        {
            conn.createStatement().execute("DROP TABLE PREMIER_LEAGUE");
            System.out.println("Tabelle Premier League gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle Premier League konnte nicht gelöscht werden: \n"+
                    e.getMessage());
        }

        //Tabelle La Liga löschen
        try
        {
            conn.createStatement().execute("DROP TABLE LALIGA");
            System.out.println("Tabelle La Liga gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle La Liga konnte nicht gelöscht werden: \n"+
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
    public void createLeagueTables()
    {
        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE Bundesliga ("+
                    "ID INT constraint bul_teamId_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"+
                    "rank INT,"+
                    "teamname VARCHAR(255) NOT NULL,"+
                    "city VARCHAR(255) NOT NULL, "+
                    "points INT )";
            stmt.execute(sql);


            sql = "CREATE TABLE Premier_League ("+
                    "ID INT constraint pl_teamId_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"+
                    "rank INT,"+
                    "teamname VARCHAR(255) NOT NULL,"+
                    "city VARCHAR(255) NOT NULL, "+
                    "points INT )";
            stmt.execute(sql);

            sql = "CREATE TABLE LaLiga ("+
                    "ID INT constraint ll_teamId_pk PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"+
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
    public void checkTablesIfCreated()
    {
        try {
            DatabaseMetaData metaData = conn.getMetaData();

            //TABLE test
            ResultSet rs = metaData.getTables(null,null,null,new String[]{"TABLE"});
            rs.next();
            assertThat(rs.getString("TABLE_NAME"),is("BUNDESLIGA"));
            rs.next();
            assertThat(rs.getString("TABLE_NAME"),is("LALIGA"));
            rs.next();
            assertThat(rs.getString("TABLE_NAME"),is("PREMIER_LEAGUE"));


            //check PK's
            rs = metaData.getPrimaryKeys(null,null,"PREMIER_LEAGUE");
            rs.next();
            assertThat(rs.getString("PK_NAME"),is("PL_TEAMID_PK"));

            rs = metaData.getPrimaryKeys(null,null,"BUNDESLIGA");
            rs.next();
            assertThat(rs.getString("PK_NAME"),is("BUL_TEAMID_PK"));

            rs = metaData.getPrimaryKeys(null,null,"LALIGA");
            rs.next();
            assertThat(rs.getString("PK_NAME"),is("LL_TEAMID_PK"));

            //check Datatype
            rs = metaData.getColumns(null,null,"BUNDESLIGA",null);
            rs.next();
            assertThat(rs.getString("DATA_TYPE"),is("4")); //4 sind INT
            rs.next();
            rs.next();
            assertThat(rs.getString("DATA_TYPE"),is("12")); //12 sind VARCHAR

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public  void insertTeamsIntoBundesliga()
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
            System.out.println("Es wurden "+countInserts+" Teams zur Bundesliga hinzugefuegt");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts,is(18));
    }

    @Test
    public  void insertTeamsIntoPremierLeague()
    {
        //Daten einfügen
        int countInserts = 0;
        try
        {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO PREMIER_LEAGUE (rank,teamname,city,points)" + " VALUES (1,'FC ARSENAL','LONDON',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO PREMIER_LEAGUE (rank,teamname,city,points)" + " VALUES (2,'MANCHESTER UNITED','MANCHESTER',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO PREMIER_LEAGUE (rank,teamname,city,points)" + " VALUES (3,'FC CHELSEA','LONDON',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO PREMIER_LEAGUE (rank,teamname,city,points)" + " VALUES (4,'MANCHESTER CITY','MANCHESTER',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO PREMIER_LEAGUE (rank,teamname,city,points)" + " VALUES (5,'LIVERPOOL','LIVERPOOL',0)";
            countInserts += stmt.executeUpdate(sql);
            System.out.println("Es wurden "+countInserts+" Teams zur Premier League hinzugefuegt");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts,is(5));
    }

    @Test
    public  void insertTeamsIntoLaLiga()
    {
        //Daten einfügen
        int countInserts = 0;
        try
        {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO LALIGA (rank,teamname,city,points)" + " VALUES (1,'FC BARCELONA','BARCELONA',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO LALIGA (rank,teamname,city,points)" + " VALUES (2,'CF REAL MADRID','MADRID',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO LALIGA (rank,teamname,city,points)" + " VALUES (3,'ATLETICO MADRID','MADRID',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO LALIGA (rank,teamname,city,points)" + " VALUES (4,'FC SEVILLA','SEVILLA',0)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO LALIGA (rank,teamname,city,points)" + " VALUES (5,'FC VALENCIA','VALENICA',0)";
            countInserts += stmt.executeUpdate(sql);
            System.out.println("Es wurden "+countInserts+" Teams zur La Liga hinzugefuegt");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts,is(5));
    }

}
