package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/transactions_db";
    private static final String USER = "root";
    private static final String PASS = "root";

    public static void main(String[] args) {
        String filePath = "D:\\Project\\InsertBatchJob\\src\\main\\resources\\dataSource.txt";
        // JDBC connection
        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASS)) {
            String insertSQL = "INSERT INTO transactions (account_number, trx_amount, description, trx_date, trx_time, customer_id) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL);
                 BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

                String line;
                // Skip the header line
                reader.readLine();

                while ((line = reader.readLine()) != null) {
                    String[] columns = line.split("\\|");

                    if (columns.length == 6) {
                        try {
                            String accountNumber = columns[0];
                            double trxAmount = Double.parseDouble(columns[1]); // Handle number format correctly
                            String description = columns[2];
                            String trxDate = columns[3];
                            String trxTime = columns[4];
                            String customerId = columns[5];

                            pstmt.setString(1, accountNumber);
                            pstmt.setDouble(2, trxAmount);
                            pstmt.setString(3, description);
                            pstmt.setString(4, trxDate);
                            pstmt.setString(5, trxTime);
                            pstmt.setString(6, customerId);

                            pstmt.addBatch();
                        } catch (NumberFormatException e) {
                            System.err.println("Skipping invalid row: " + line);
                        }
                    } else {
                        System.err.println("Skipping invalid row: " + line);
                    }
                }

                pstmt.executeBatch();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}