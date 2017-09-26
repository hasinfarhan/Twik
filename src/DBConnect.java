import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

public class DBConnect
{
  private Connection connection;

  public DBConnect(String host,String dbname,String username,String password, String database)
  {

    if(database.equalsIgnoreCase("mysql"))
    {
      //System.out.println ("In MySql");
      String url="jdbc:mysql://" + host + ":3306/" + dbname+"?autoReconnect=true&useSSL=false";
      try
      {
        Class.forName("com.mysql.jdbc.Driver");
        this.connection=DriverManager.getConnection(url,username,password);
      }
      catch(Exception e)
      {
        System.out.println("Exception in DatabaseConnect: " + e);
        System.exit(0);
      }
    }
    else if(database.equalsIgnoreCase("oracle"))
    {
      String url="jdbc:oracle:thin:@//"+host+":1521/"+dbname;
      try
      {
        Class.forName("oracle.jdbc.OracleDriver");
        this.connection=DriverManager.getConnection(url,username,password);
      }
      catch(Exception e)
      {
        System.out.println("Exception in DatabaseConnect: " + e);
        System.exit(0);
      }
    }
    else if(database.equalsIgnoreCase("sqlserver"))
    {
      String url="jdbc:microsoft:sqlserver://"+host+":1433"+";DatabaseName="+dbname;
      try
      {
        Class.forName("com.microsoft.jdbc.sqlserver.SQLServerDriver");
        this.connection=DriverManager.getConnection(url,username,password);
      }
      catch(Exception e)
      {
        System.out.println("Exception in DatabaseConnect: " + e);
        System.exit(0);
      }
    }
    else if(database.equalsIgnoreCase("postgresql"))
    {
      String url = "jdbc:postgresql://" + host +":5432/ " + dbname;
      try
      {
        Class.forName("org.postgresql.Driver");
        this.connection=DriverManager.getConnection(url,username,password);
      }
      catch(Exception e)
      {
        System.out.println("Exception in DatabaseConnect: " + e);
        System.exit(0);
      }
    }
    else {
      System.out.println("Invalid Database");
      System.exit(0);
    }

  }
  public int updateDB(String query) throws Exception
  {
    Statement statment = this.connection.createStatement();
    return statment.executeUpdate(query);
  }

  public ResultSet searchDB(String query) throws Exception
  {
    Statement statement = this.connection.createStatement();
    return statement.executeQuery(query);
  }

  public void close()
  {
    try
    {
      this.connection.close();
    }
    catch(Exception e)
    {
      System.out.println("Exception in close: " + e);
    }
  }

}




