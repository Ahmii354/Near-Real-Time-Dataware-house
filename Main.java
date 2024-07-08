// 21i 1758
// Ahmed Hassan
// DS-M

import java.sql.*;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
class Sale {
    int TRANSACTION_ID;
    String T_DATE;
    int PRODUCT_ID;
    int CUSTOMER_ID;
    String CUSTOMER_NAME;
    String GENDER;
    int QUANTITY;
    String PRODUCT_NAME;
    int SUPPLIER_ID;
    String SUPPLIER_NAME;
    String Product_Price;
    String StoreName;
    int StoreID;

    float total_price;
    public void addAttributes(MasterData masterData) {
        this.PRODUCT_NAME = masterData.PRODUCT_NAME;
        this.SUPPLIER_ID = masterData.SUPPLIER_ID;
        this.SUPPLIER_NAME = masterData.SUPPLIER_NAME;
        this.Product_Price = masterData.PRODUCT_PRICE;
        this.StoreName = masterData.STORE_NAME;
        this.StoreID = masterData.STORE_ID;
        String[] priceParts = masterData.PRICE.split("\\$");
        if (priceParts.length > 0) {
            this.total_price = this.QUANTITY * Float.parseFloat(priceParts[0]);
        }
        //	this.TOTAL_SALE = this.QUANTITY * masterData.PRICE;
    }
}

class MasterData {
    int PRODUCT_ID;
    String PRODUCT_NAME;
    String PRODUCT_PRICE;
    int SUPPLIER_ID;
    String SUPPLIER_NAME;
    int STORE_ID;
    String STORE_NAME;

    String PRICE;
}

public class Main {
    public static void main(String args[]) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter the username for the transaction database:");
        String transactionDbUsername = scanner.nextLine();

        System.out.println("Enter the username for the master data database:");
        String masterDataDbUsername = scanner.nextLine();

        System.out.println("Enter the username for the data warehouse database:");
        String dataWarehouseDbUsername = scanner.nextLine();
        Queue<Sale> streamBuffer = new LinkedList<>(); //queue for stream Buffer
        try {Class.forName("com.mysql.jdbc.Driver");
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/transection", transactionDbUsername, "ahmii123");//pass ahmii123

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM transactions");
            loadDates(dataWarehouseDbUsername);
            // Read data from ResultSet and add to stream buffer
            while (rs.next()) {
                Sale sale = new Sale();
                sale.TRANSACTION_ID = rs.getInt("OrderID");
                sale.T_DATE = rs.getString("OrderDate");
                sale.PRODUCT_ID = rs.getInt("ProductID");
                sale.CUSTOMER_ID = rs.getInt("CustomerID");
                sale.CUSTOMER_NAME = rs.getString("CustomerName");
                sale.GENDER = rs.getString("Gender");
                sale.QUANTITY = rs.getInt("QuantityOrdered");

                streamBuffer.add(sale);
                //System.out.println("Data loaded into the queue: " + sale.toString());
            }

            //Process the stream buffer
            //processStreamBuffer(streamBuffer);
            //processStreamBuffer(streamBuffer);
            processStreamBufferInBatches(streamBuffer, masterDataDbUsername);

            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //loadDates();
    }
    static int time_id = 1;
    static int quarter = -1;
    private static void loadDates(String dataWarehouseDbUsername) throws SQLException {

        Connection conn_dw = DriverManager.getConnection("jdbc:mysql://localhost:3306/ELECTRONICA_DW",dataWarehouseDbUsername , "ahmii123"); //ahmii123
        PreparedStatement stmt = conn_dw.prepareStatement(
                "INSERT IGNORE INTO ELECTRONICA_DW.time (TIME_ID, DAY_OF_MONTH, DAY_OF_WEEK, MONTH, QUARTER, YEAR) " +
                        "VALUES (?, ?, ?, ?, ?, ?)");



        for (LocalDate date = LocalDate.parse("2019-01-01"); date.isBefore(LocalDate.parse("2020-01-01")); date = date.plusDays(1)) {
            // YEAR
            stmt.setInt(6, date.getYear());
            // QUARTER
            if (date.getMonthValue() >= 1 && date.getMonthValue() <= 3) quarter = 1;
            else if (date.getMonthValue() >= 4 && date.getMonthValue() <= 6) quarter = 2;
            else if (date.getMonthValue() >= 7 && date.getMonthValue() <= 9) quarter = 3;
            else if (date.getMonthValue() >= 10 && date.getMonthValue() <= 12) quarter = 4;
            stmt.setInt(5, quarter);
            // MONTH
            stmt.setInt(4, date.getMonthValue());
            // DAY_MONTH
            stmt.setInt(2, date.getDayOfMonth());
            // DAY_WEEK
            stmt.setString(3, date.getDayOfWeek().toString());
            // TIME_ID
            stmt.setInt(1, time_id++);



            stmt.executeUpdate();
        }
        conn_dw.close();
    }

    public static void shipToDW(Sale transaction){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn_dw = DriverManager.getConnection("jdbc:mysql://localhost:3306/ELECTRONICA_DW", "root", "ahmii123r"); //ahmii123
            // PRODUCT
            PreparedStatement stmt = conn_dw.prepareStatement("INSERT IGNORE INTO ELECTRONICA_DW.product VALUES (?, ?)");
            stmt.setInt(1, transaction.PRODUCT_ID);
            stmt.setString(2, transaction.PRODUCT_NAME);
            stmt.executeUpdate();
            // SUPPLIER
            stmt = conn_dw.prepareStatement("INSERT IGNORE INTO ELECTRONICA_DW.supplier VALUES (?, ?)");
            stmt.setInt(1, transaction.SUPPLIER_ID);
            stmt.setString(2, transaction.SUPPLIER_NAME);
            stmt.executeUpdate();
            // STORE
            stmt = conn_dw.prepareStatement("INSERT IGNORE INTO ELECTRONICA_DW.store VALUES (?, ?)");
            stmt.setInt(1, transaction.StoreID);
            stmt.setString(2, transaction.StoreName);
            stmt.executeUpdate();
            // CUSTOMER
            stmt = conn_dw.prepareStatement("INSERT IGNORE INTO ELECTRONICA_DW.customer VALUES (?, ?)");
            stmt.setInt(1, transaction.CUSTOMER_ID);
            stmt.setString(2, transaction.CUSTOMER_NAME);
            stmt.executeUpdate();

            // GETTING TIME_ID
            String date = transaction.T_DATE; // "transaction.T_DATE" contains the date string
            String[] dateAndTime = date.split(" "); // Splitting date and time parts using space

            String[] dateSplit = dateAndTime[0].split("/"); // Splitting date part using "/"

            // Extracted date components
            int day = Integer.parseInt(dateSplit[0]);
            int month = Integer.parseInt(dateSplit[1]);
            int year = Integer.parseInt("20" + dateSplit[2]);
            if (month < 1) {
                month = 1;
                transaction.T_DATE = dateSplit[0] + "/01" + "/" + dateSplit[2] + " " + dateAndTime[1];
            }
            if (month > 12) {
                month = 12;
                transaction.T_DATE = dateSplit[0] + "/12" + "/" + dateSplit[2] + " " + dateAndTime[1];
            }
            if (year > 2019 || year < 2019) {
                year = 2019;
                transaction.T_DATE = dateSplit[0] + "/" + dateSplit[1] + "/" + "2019 " + dateAndTime[1];
            }
            ResultSet time_rs = stmt.executeQuery("SELECT time.TIME_ID FROM ELECTRONICA_DW.time where time.DAY_OF_MONTH = "
                    +  day + " and time.MONTH = " + month + " and time.YEAR = " +  year + ";");
            time_rs.next();
            int time_id = time_rs.getInt(1);
            // SALES
            stmt = conn_dw.prepareStatement("INSERT IGNORE INTO ELECTRONICA_DW.sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setFloat(7, transaction.QUANTITY);stmt.setFloat(8, transaction.total_price);
            stmt.setInt(1, transaction.TRANSACTION_ID);stmt.setInt(2, transaction.CUSTOMER_ID);
            stmt.setInt(3, transaction.StoreID);stmt.setInt(4, transaction.PRODUCT_ID);
            stmt.setInt(5, transaction.SUPPLIER_ID);stmt.setInt(6, time_id);
            stmt.executeUpdate();
            conn_dw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processStreamBufferInBatches(Queue<Sale> streamBuffer,String masterDataDbUsername) {
        int batchSize = 1000; // batch size
        int count = 0;

        while (!streamBuffer.isEmpty()) {
            Queue<Sale> batch = new LinkedList<>();

            // Populate the batch with the next set of records
            while (!streamBuffer.isEmpty() && count < batchSize) {
                batch.add(streamBuffer.poll());
                count++;
            }

            // Process the batch
            processStreamBuffer(batch, masterDataDbUsername);

            // Introduce a delay
            try {
                Thread.sleep(2000); // 2 seconds delay
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            count = 0; // Reset count for the next batch
        }
    }

    private static void processStreamBuffer(Queue<Sale> streamBuffer, String masterDataDbUsername) {
        Map<Integer, LinkedList<Sale>> multiHashTable = new HashMap<>();

        // Process each sale in the stream buffer
        while (!streamBuffer.isEmpty()) {
            Sale sale = streamBuffer.poll();
            int productID = sale.PRODUCT_ID;
            if (!multiHashTable.containsKey(productID)) {
                multiHashTable.put(productID, new LinkedList<>());
            }
            multiHashTable.get(productID).add(sale);
        }
        int count = 1;
        while (count < 102) {
            int oldestProductID = getOldestProductID(multiHashTable);
            //System.out.println("OLdest ID: " + oldestProductID);
            //LinkedList<MasterData> diskBuffer = loadMasterDataSegment(count);
            LinkedList<MasterData> diskBuffer = loadMasterDataSegment(oldestProductID, masterDataDbUsername);
            matchAndProduceOutput(streamBuffer, multiHashTable, diskBuffer);
            count++;
            if (oldestProductID == -1) {
                break;

            }
            //System.out.println("Output Tuple: " + val);
        }
        //System.out.println("Output Tuple: " + val);

    }

    static int val = 0;
    private static Map<Integer, Integer> saleCounts = new HashMap<>();

    private static void matchAndProduceOutput(Queue<Sale> streamBuffer, Map<Integer, LinkedList<Sale>> multiHashTable, LinkedList<MasterData> diskBuffer) {
        // Process each record in the disk buffer
        for (MasterData masterData : diskBuffer) {
            int productID = masterData.PRODUCT_ID;
            // System.out.println("ID : " + productID);
            // Check if the productID exists in the multi-hash table
            if (multiHashTable.containsKey(productID)) {
                LinkedList<Sale> saleQueue = multiHashTable.get(productID);

                // Check if the queue is not empty
                while (!saleQueue.isEmpty()) {
                    // Get the oldest Sale record from the queue
                    Sale sale = saleQueue.poll();
                    // Add required attributes of MD into the transaction tuple
                    sale.addAttributes(masterData);
                    sale.addAttributes(masterData);
                    //if (val < 51) {
                    System.out.println(val + "." + " PRODUCT_ID:" + sale.PRODUCT_ID
                            + " PRODUCT_NAME:" + sale.PRODUCT_NAME + " CUSTOMER_ID:" + sale.CUSTOMER_ID
                            + " CUSTOMER_NAME:" + sale.CUSTOMER_NAME + " T_DATE" + sale.T_DATE
                            + " QUANTITY:" + sale.QUANTITY+ " SUPPLIER_NAME:" + sale.SUPPLIER_NAME
                            + " SUPPLIER_ID:" + sale.SUPPLIER_ID + " GENDER:" + sale.GENDER ); //}
                    shipToDW(sale);
                    val++;
                    // Increment the count for the specific product ID
                    saleCounts.compute(productID, (key, count) -> (count == null) ? 1 : count + 1);
                    // Produce the output tuple
                    // System.out.println("Output Tuple: " + sale.PRODUCT_ID);
                    // Remove the matched tuple from the multi-hash table along with its join attribute value from the queue
                    multiHashTable.remove(productID);
                    // Check if there are more transactions for the same productID in the multiHashTable
                    if (!saleQueue.isEmpty()) {
                        // Handle the case where there are more than one transaction for the same productID
                        while (!saleQueue.isEmpty()) {
                            Sale additionalSale = saleQueue.poll();
                            additionalSale.addAttributes(masterData);

                            // Increment the count for the specific product ID
                            saleCounts.compute(productID, (key, count) -> (count == null) ? 1 : count + 1);
                            //if (val < 51) { //to print top 50
                            System.out.println(val + "." + " PRODUCT_ID:" + additionalSale.PRODUCT_ID
                                    + " PRODUCT_NAME:" + additionalSale.PRODUCT_NAME + " CUSTOMER_ID:" + additionalSale.CUSTOMER_ID
                                    + " CUSTOMER_NAME:" + additionalSale.CUSTOMER_NAME + " T_DATE" + sale.T_DATE
                                    + " GENDER:" + additionalSale.GENDER + " QUANTITY:" + additionalSale.QUANTITY
                                    + " SUPPLIER_ID:" + additionalSale.SUPPLIER_ID + " SUPPLIER_NAME:" + additionalSale.SUPPLIER_NAME);//}
                            shipToDW(additionalSale);
                            val++;
                        }
                    }
                }
            }
        }
    }

    private static int getOldestProductID(Map<Integer, LinkedList<Sale>> multiHashTable) {
        int oldestProductID = 1;
        for (LinkedList<Sale> saleQueue : multiHashTable.values()) {
            if (!saleQueue.isEmpty()) {
                Sale oldestSale = saleQueue.poll(); // Retrieve and remove the head of the queue
                oldestProductID = oldestSale.PRODUCT_ID;
                break;
            }
        }
        return oldestProductID;
    }

    private static LinkedList<MasterData> loadMasterDataSegment(int startingProductID,String masterDataDbUsername) {
        LinkedList<MasterData> diskBuffer = new LinkedList<>();

        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/masterdata", masterDataDbUsername, "ahmii123");

            // PreparedStatement to fetch Master Data
            String sql = "SELECT * FROM master_data WHERE productID >= ? ORDER BY productID ASC LIMIT 10;";
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setInt(1, startingProductID);
                ResultSet rs = pstmt.executeQuery();

                // Process Master Data and load into disk buffer
                while (rs.next()) {
                    MasterData masterData = new MasterData();
                    masterData.PRODUCT_ID = rs.getInt("productID");
                    masterData.PRODUCT_NAME = rs.getString("productName");
                    masterData.PRODUCT_PRICE = rs.getString("productPrice");
                    masterData.SUPPLIER_ID = rs.getInt("supplierID");
                    masterData.SUPPLIER_NAME = rs.getString("supplierName");
                    masterData.STORE_ID = rs.getInt("storeID");
                    masterData.STORE_NAME = rs.getString("storeName");
                    masterData.PRICE = rs.getString("productPrice");

                    // Load masterData into disk buffer
                    diskBuffer.add(masterData);
                    //System.out.println("Master Data loaded into the disk buffer: " + masterData.toString());
                }
                //System.out.println("complete " + 10);
            }

            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return diskBuffer;
    }}