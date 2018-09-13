package javatools.database;

import amie.data.KB;

import java.sql.*;
import java.util.ArrayList;

public class Virtuoso {

    /**
     *
     */
    private static final String databasePath = "jdbc:virtuoso://134.169.32.169:1111";
    private static final String user = "dba";
    private static final String password = "S4g230Bl";

    protected Connection connection;


    public Virtuoso(){
        this.connect();
    }

    public void connect(){
        if(connection == null) {
            try {
                connection = DriverManager.getConnection(databasePath, user, password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
            else{
                try {
                if(connection.isClosed()){
                        connection = DriverManager.getConnection(databasePath,user,password);
                    }
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }

    }

    private ArrayList<String> getResults(String sparqlQuery){

        try {
            if(connection == null || this.connection.isClosed()){
                this.connect();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ArrayList<String> result = new ArrayList<>();
        try(PreparedStatement stmt = connection.prepareStatement("SPARQL " + sparqlQuery)) {

            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();
            if (!rs.isBeforeFirst()) {
                return result;
            }
            else {
                while (rs.next()) {
                    String resultString = "";
                    if(columnCount==1){
                        result.add(rs.getString(1));
                    }
                    else{
                        for(int i = 1; i <= columnCount - 1; i++) {
                            resultString += rs.getString(i) + " ";
                        }
                        resultString += rs.getString(columnCount);
                        result.add(resultString);
                    }

                }
            }
            stmt.close();
        } catch (SQLException e) {
           e.printStackTrace();
        }
        return result;

    }

    public int getResultSize(String sparqlQuery){
        try {
            if(connection == null || this.connection.isClosed()){
                this.connect();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long start = System.currentTimeMillis();

        int size = -1;
        sparqlQuery = sparqlQuery.replace("*", "COUNT(DISTINCT " +
                "w?a)");

        try(PreparedStatement stmt = connection.prepareStatement("SPARQL " + sparqlQuery)) {
            ResultSet rs = stmt.executeQuery();
            rs.next();
            size = rs.getInt(1);



            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println(sparqlQuery + " took " + (end-start) + "ms");
        return size;

    }

    public int getSubjectSize(String URI){
        try {
            if(connection == null || this.connection.isClosed()){
                this.connect();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int size = -1;
        String sparqlQuery = "SELECT COUNT DISTINCT(?s) WHERE {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + URI + "}";

        try(PreparedStatement stmt = connection.prepareStatement("SPARQL " + sparqlQuery)) {

            ResultSet rs = stmt.executeQuery();
            rs.next();
            size = rs.getInt(1);



            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return size;

    }


    public ResultSet query(String sparql){
        try {
            if(connection == null || this.connection.isClosed()){
                this.connect();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        ResultSet results = null;
        try{
            Statement stmt = connection.createStatement();
            sparql = "SPARQL " + sparql;
            sparql = sparql + "";
            results = stmt.executeQuery(sparql);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return results;
    }



    public static void main(String[] args) throws SQLException {

        Virtuoso virtuoso = new Virtuoso();
        String sparql = "SELECT * WHERE {?s ?p ?o} LIMIT 5";

        System.out.println(virtuoso.connection.isClosed());
//        for(String s : virtuoso.getResults(sparql)){
//            System.out.println(s);
//        }
        System.out.println(virtuoso.getResultSize(sparql));
    }


}
