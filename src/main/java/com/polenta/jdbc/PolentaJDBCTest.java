package com.polenta.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.String.format;

public class PolentaJDBCTest {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("com.polenta.jdbc.Driver");

    Properties connectionProps = new Properties();
    connectionProps.put("user", "");
    connectionProps.put("password", "");

    String connectionURL = format("jdbc:polenta://%s:%s/", "localhost", "3110");

    Connection conn = DriverManager.getConnection(connectionURL, connectionProps);

    createBag(conn);
    try {
      insertBag(conn);
      selectBag(conn);
    } finally {
      dropBag(conn);
    }
  }

  private static void createBag(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    boolean result = stmt.execute("CREATE BAG person (name STRING)");
    assert result == true;
  }

  private static void insertBag(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    boolean result = stmt.execute("INSERT INTO person (name) VALUES ('John')");
    assert result == true;
  }

  private static void selectBag(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT name FROM person");
    rs.first();
    System.out.println(rs.getString(0));
  }

  private static void dropBag(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    boolean result = stmt.execute("DROP person");
    assert result == true;
  }

}
