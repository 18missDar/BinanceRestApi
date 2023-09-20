package com.demo;

import com.binance.connector.client.WebSocketStreamClient;
import com.binance.connector.client.impl.WebSocketStreamClientImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import org.postgresql.util.PSQLException;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Service
public class RestApi {
    private Timer timer;
    private List<TradeEvent> tradeEvents = new ArrayList<>();
    private String PROCEED_TRADE_EVENT;
    private String SOURCE_TRADE_EVENT;
    private Boolean startTracking = true;
    private DatabaseConfig databaseConfig;
    private AppConfig appConfig;
    private WebSocketStreamClient wsStreamClient;
    private int number_connection_socket;

    public void startRestApi(DatabaseConfig databaseConfig, AppConfig appConfig, boolean update_parameter) {
        this.databaseConfig = databaseConfig;
        this.appConfig = appConfig;
        if (!update_parameter) {
            PROCEED_TRADE_EVENT = "processed_trade_event_" + appConfig.getEventSymbol() + "_";
            SOURCE_TRADE_EVENT = "source_trade_event_" + appConfig.getEventSymbol() + "_";
            createSourceTable();
            createSummaryTable();
        }
        else {
            startTracking = true;
            findLastCreatedTable();
        }
        startTradeStream();
    }


    public void findLastCreatedTable() {
        String sourcePattern = "^source_trade_event_" + appConfig.getEventSymbol() + "_([0-9]+)$";
        String proceedPattern = "^processed_trade_event_" + appConfig.getEventSymbol() + "_([0-9]+)$";
        Pattern sourceTableNamePattern = Pattern.compile(sourcePattern);
        Pattern proceedTableNamePattern = Pattern.compile(proceedPattern);

        List<String> sourceTradeEventsNames = new ArrayList<>();
        List<String> proceedTradeEventsNames = new ArrayList<>();

        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword());
             Statement statement = connection.createStatement()) {
            String showTablesQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            try (ResultSet resultSet = statement.executeQuery(showTablesQuery)) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString(1);
                    if (sourceTableNamePattern.matcher(tableName).matches()) {
                        sourceTradeEventsNames.add(tableName);
                    } else if (proceedTableNamePattern.matcher(tableName).matches()) {
                        proceedTradeEventsNames.add(tableName);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Find the element with the maximum end number for sourceTradeEventsNames
        String maxSourceTableName = null;
        long maxSourceNumber = Long.MIN_VALUE;
        for (String tableName : sourceTradeEventsNames) {
            Matcher matcher = sourceTableNamePattern.matcher(tableName);
            if (matcher.matches()) {
                long number = Long.parseLong(matcher.group(1));
                if (number > maxSourceNumber) {
                    maxSourceTableName = tableName;
                    maxSourceNumber = number;
                }
            }
        }

        // Find the element with the maximum end number for proceedTradeEventsNames
        String maxProceedTableName = null;
        long maxProceedNumber = Long.MIN_VALUE;
        for (String tableName : proceedTradeEventsNames) {
            Matcher matcher = proceedTableNamePattern.matcher(tableName);
            if (matcher.matches()) {
                long number = Long.parseLong(matcher.group(1));
                if (number > maxProceedNumber) {
                    maxProceedTableName = tableName;
                    maxProceedNumber = number;
                }
            }
        }

        // Now you can use maxSourceTableName and maxProceedTableName as needed.
        SOURCE_TRADE_EVENT = maxSourceTableName;
        PROCEED_TRADE_EVENT = maxProceedTableName;
    }


    private void createSourceTable() {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            // Generate the table name with the current time in Unix format
            long currentTimeUnix = System.currentTimeMillis() / 1000L;
            SOURCE_TRADE_EVENT = SOURCE_TRADE_EVENT + currentTimeUnix;

            String createQuery = "CREATE TABLE " + SOURCE_TRADE_EVENT + " (" +
                    "event_type VARCHAR(255), " +
                    "event_time BIGINT, " +
                    "symbol VARCHAR(255), " +
                    "trade_id BIGINT PRIMARY KEY, " + // Added PRIMARY KEY constraint for trade_id
                    "price VARCHAR(255), " +
                    "quantity VARCHAR(255), " +
                    "buyer_order_id BIGINT, " +
                    "seller_order_id BIGINT, " +
                    "trade_time BIGINT, " +
                    "is_buyer_market_maker BOOLEAN" +
                    ")";

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createQuery);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private void createSummaryTable() {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            // Generate the table name with the current timestamp
            long currentTimeUnix = System.currentTimeMillis() / 1000L;
            PROCEED_TRADE_EVENT = PROCEED_TRADE_EVENT + currentTimeUnix;

            String createTableQuery = "CREATE TABLE IF NOT EXISTS " + PROCEED_TRADE_EVENT + " (" +
                    "id SERIAL PRIMARY KEY," +
                    "event_type VARCHAR(50)," +
                    "symbol VARCHAR(50)," +
                    "average_price_m_true VARCHAR(50)," +
                    "average_price_m_false VARCHAR(50)," +
                    "summary_quantity_m_true VARCHAR(50)," +
                    "summary_quantity_m_false VARCHAR(50)," +
                    "trade_time BIGINT" +
                    ")";

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createTableQuery);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean insertTradeEventToSourceTable(TradeEvent tradeEvent) {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {

            String insertQuery = "INSERT INTO " + SOURCE_TRADE_EVENT + " (event_type, event_time, symbol, trade_id, " +
                    "price, quantity, buyer_order_id, seller_order_id, trade_time, is_buyer_market_maker) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
                statement.setString(1, tradeEvent.getEventType());
                statement.setLong(2, tradeEvent.getEventTime());
                statement.setString(3, tradeEvent.getSymbol());
                statement.setLong(4, tradeEvent.getTradeId());
                statement.setString(5, tradeEvent.getPrice());
                statement.setString(6, tradeEvent.getQuantity());
                statement.setLong(7, tradeEvent.getBuyerOrderId());
                statement.setLong(8, tradeEvent.getSellerOrderId());
                statement.setLong(9, tradeEvent.getTradeTime());
                statement.setBoolean(10, tradeEvent.isBuyerMarketMaker());

                statement.executeUpdate();
                return true;
            }
        } catch (PSQLException e) {
            return false;
        } catch (SQLException e) {
            return false;
        }
    }

    private void startTradeStream() {
        this.wsStreamClient = new WebSocketStreamClientImpl();
        ArrayList<String> streams = new ArrayList<>();

        streams.add(appConfig.getEventSymbol() + "@trade");

        this.number_connection_socket = wsStreamClient.combineStreams(streams, (event) -> {
            TradeEvent tradeEvent = parseTradeEvent(event);
            if (tradeEvent != null) {
                    if (insertTradeEventToSourceTable(tradeEvent))
                        tradeEvents.add(tradeEvent);
                    if (startTracking) {
                        // Start the timer when tradeTime ends with 00 seconds
                        tradeEvents.clear();
                        startTracking = false;
                        startTimer();
                    }
            }
        });

        // Schedule the WebSocket renewal before it expires
        scheduleWebSocketRenewal();
    }

    private void startTimer() {
        if (timer != null) {
            timer.cancel();
        }

        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (!tradeEvents.isEmpty()) {
                    calculateAndPrintSummary();
                    tradeEvents.clear();
                }
            }
        }, 60000, 60000);
    }

    private void scheduleWebSocketRenewal() {
        // Determine the lifetime of the WebSocket connection provided by the data provider.
        // For example, if it expires after 24 hours, you could schedule the renewal 23.5 hours from now.
        long renewalTimeMillis = System.currentTimeMillis() + (23 * 60 * 60 * 1000) + (30 * 60 * 1000); // 23.5 hours from now

        // Use a Timer or Scheduler to trigger the WebSocket renewal at the scheduled time
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                startTradeStream(); // Renew the WebSocket connection
                wsStreamClient.closeConnection(number_connection_socket);
            }
        }, new Date(renewalTimeMillis));
    }

    private TradeEvent parseTradeEvent(String event) {
        try {
            // Parse the JSON event
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(event);

            // Extract the necessary fields
            JsonNode dataNode = jsonNode.get("data");
            String eventType = dataNode.get("e").asText();
            long eventTime = dataNode.get("E").asLong();
            String symbol = dataNode.get("s").asText();
            long tradeId = dataNode.get("t").asLong();
            String price = dataNode.get("p").asText();
            String quantity = dataNode.get("q").asText();
            long buyerOrderId = dataNode.get("b").asLong();
            long sellerOrderId = dataNode.get("a").asLong();
            long tradeTime = dataNode.get("T").asLong();
            boolean isBuyerMarketMaker = dataNode.get("m").asBoolean();

            // Create and return a TradeEvent object
            return new TradeEvent(eventType, eventTime, symbol, tradeId, price, quantity,
                    buyerOrderId, sellerOrderId, tradeTime, isBuyerMarketMaker);
        } catch (Exception e) {
            e.printStackTrace();
            return null; // Return null in case of any parsing errors
        }
    }


    private void saveSummaryToDatabase(JsonObject summaryJson) {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            String insertQuery = "INSERT INTO " + PROCEED_TRADE_EVENT + " (event_type, symbol, average_price_m_true, " +
                    "average_price_m_false, summary_quantity_m_true, summary_quantity_m_false, trade_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
                statement.setString(1, summaryJson.get("e").getAsString());
                statement.setString(2, summaryJson.get("s").getAsString());
                statement.setString(3, summaryJson.get("p_m_true").getAsString());
                statement.setString(4, summaryJson.get("p_m_false").getAsString());
                statement.setString(5, summaryJson.get("q_m_true").getAsString());
                statement.setString(6, summaryJson.get("q_m_false").getAsString());
                statement.setLong(7, summaryJson.get("T").getAsLong());

                statement.executeUpdate();
            }
        } catch (PSQLException e) {
            //nothing
        } catch (SQLException e) {
            //nothing
        }
    }

    private void calculateAndPrintSummary() {
        if (tradeEvents.size() >= 1) {
            String symbol = tradeEvents.get(0).getSymbol();
            long tradeTime = tradeEvents.get(0).getTradeTime();
            double sumQuantityTrue = 0;
            double sumQuantityFalse = 0;
            double weightedSumPriceTrue = 0;
            double weightedSumPriceFalse = 0;

            for (TradeEvent tradeEvent : tradeEvents) {
                double price = Double.parseDouble(tradeEvent.getPrice());
                double quantity = Double.parseDouble(tradeEvent.getQuantity());
                boolean isBuyerMarketMaker = tradeEvent.isBuyerMarketMaker();

                if (isBuyerMarketMaker) {
                    sumQuantityTrue += quantity;
                    weightedSumPriceTrue += price * quantity; // Calculate weighted sum for true
                } else {
                    sumQuantityFalse += quantity;
                    weightedSumPriceFalse += price * quantity; // Calculate weighted sum for false
                }
            }

            double weightedAveragePriceTrue = weightedSumPriceTrue / sumQuantityTrue; // Calculate weighted average for true
            double weightedAveragePriceFalse = weightedSumPriceFalse / sumQuantityFalse; // Calculate weighted average for false

            // Create the summary JSON object
            JsonObject summaryJson = new JsonObject();
            summaryJson.addProperty("e", "trade");
            summaryJson.addProperty("s", symbol);
            summaryJson.addProperty("p_m_true", String.format("%.8f", weightedAveragePriceTrue));
            summaryJson.addProperty("p_m_false", String.format("%.8f", weightedAveragePriceFalse));
            summaryJson.addProperty("q_m_true", String.valueOf(sumQuantityTrue));
            summaryJson.addProperty("q_m_false", String.valueOf(sumQuantityFalse));
            summaryJson.addProperty("T", tradeTime);

            // Print the summary JSON object
            saveSummaryToDatabase(summaryJson);
        }
    }
}