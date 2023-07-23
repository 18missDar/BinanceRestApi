package com.demo;

import com.binance.connector.client.WebSocketStreamClient;
import com.binance.connector.client.impl.WebSocketStreamClientImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.springframework.stereotype.Service;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

@Service
public class OrderBookManager {

    private String PROCEED_ORDER_BOOK;
    private String SOURCE_ORDER_BOOK_EVENT;
    private String DEPTH_API_URL;
    private final DatabaseConfig databaseConfig;
    private final AppConfig appConfig;

    private long lastUpdateId;

    private List<OrderBookEvent> orderBookEventsList;
    private List<OrderBookEvent.PriceQuantityPair> bidsActual;
    private List<OrderBookEvent.PriceQuantityPair> asksActual;
    private Boolean startTracking = true;

    public List<OrderBookEvent.PriceQuantityPair> getBidsActual() {
        return bidsActual;
    }

    public List<OrderBookEvent.PriceQuantityPair> getAsksActual() {
        return asksActual;
    }

    public long getLastUpdateId() {
        return lastUpdateId;
    }

    public OrderBookManager(DatabaseConfig databaseConfig, AppConfig appConfig) {
        this.databaseConfig = databaseConfig;
        this.appConfig = appConfig;
        orderBookEventsList = new ArrayList<>();
        bidsActual = new ArrayList<>();
        asksActual = new ArrayList<>();
        SOURCE_ORDER_BOOK_EVENT = "source_order_book_event_";
        PROCEED_ORDER_BOOK = "proceed_order_book_event_";
        DEPTH_API_URL = "https://api.binance.com/api/v3/depth?symbol=" + appConfig.getEventSymbol().toUpperCase(Locale.ROOT) + "&limit=" + appConfig.getLimitCount();
        createSourceTable();
        createProceedTable();
        startOrderBookEventStream();
    }

    private void startOrderBookEventStream(){
        WebSocketStreamClient wsStreamClient = new WebSocketStreamClientImpl();
        ArrayList<String> streams = new ArrayList<>();
        streams.add(appConfig.getEventSymbol() + "@depth");

        wsStreamClient.combineStreams(streams, (event) -> {
            OrderBookEvent orderBookEvent = parseOrderBookEvent(event);
            if (orderBookEvent != null) {
                orderBookEventsList.add(orderBookEvent);

                if (startTracking) {
                    getDepthSnapshot();
                    startTracking = false;
                    return;
                }

                // Step 4: Drop any event where u is <= lastUpdateId in the snapshot
                if (orderBookEvent.getFinalUpdateId() <= lastUpdateId) {
                    return;
                }

                // Step 5: The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1
                if (orderBookEvent.getFirstUpdateId() == lastUpdateId + 1) {
                    updateLocalOrderBook(orderBookEvent);
                }

                lastUpdateId += 1;

            } else {
                // Handle parsing errors
                System.err.println("Failed order book event or doubled in cached list: " + event);
            }
        });

        // Schedule the WebSocket renewal before it expires
        scheduleWebSocketRenewal();
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
                startOrderBookEventStream(); // Renew the WebSocket connection
            }
        }, new Date(renewalTimeMillis));
    }

    private void createSourceTable() {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            // Generate the table name with the current time in Unix format
            long currentTimeUnix = System.currentTimeMillis() / 1000L;
            SOURCE_ORDER_BOOK_EVENT = SOURCE_ORDER_BOOK_EVENT + currentTimeUnix;

            String createQuery = "CREATE TABLE " + SOURCE_ORDER_BOOK_EVENT + " (" +
                    "event_type VARCHAR(50), " +
                    "event_time BIGINT, " +
                    "symbol VARCHAR(10), " +
                    "first_update_id BIGINT, " +
                    "final_update_id BIGINT, " +
                    "bids JSONB, " +
                    "asks JSONB" +
                    ")";

            try (PreparedStatement statement = connection.prepareStatement(createQuery)) {
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createProceedTable() {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            // Generate the table name with the current time in Unix format
            long currentTimeUnix = System.currentTimeMillis() / 1000L;
            PROCEED_ORDER_BOOK = PROCEED_ORDER_BOOK + currentTimeUnix;

            String createQuery = "CREATE TABLE " + PROCEED_ORDER_BOOK + " (" +
                    "lastUpdateId BIGINT, " +
                    "partial_order_book JSONB" +
                    ")";

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createQuery);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateLocalOrderBook(OrderBookEvent event) {
        // Update the local order book based on the event data
        List<OrderBookEvent.PriceQuantityPair> bidsToUpdate = event.getBids();
        List<OrderBookEvent.PriceQuantityPair> asksToUpdate = event.getAsks();

        // Remove price levels with quantity = 0
        bidsToUpdate.removeIf(pair -> pair.getQuantity().equals("0"));
        asksToUpdate.removeIf(pair -> pair.getQuantity().equals("0"));

        // Step 8: Receiving an event that removes a price level that is not in your local order book can happen and is normal
        // Update bidsActual and asksActual based on the event data
        for (OrderBookEvent.PriceQuantityPair bid : bidsToUpdate) {
            bidsActual.removeIf(pair -> pair.getPrice().equals(bid.getPrice()));
            if (!bid.getQuantity().equals("0")) {
                bidsActual.add(bid);
            }
        }

        for (OrderBookEvent.PriceQuantityPair ask : asksToUpdate) {
            asksActual.removeIf(pair -> pair.getPrice().equals(ask.getPrice()));
            if (!ask.getQuantity().equals("0")) {
                asksActual.add(ask);
            }
        }

        // Update the local database table with the updated bids and asks
        String resultJson = "{ \"bids\": " + bidsActual.toString() + ", \"asks\": " + asksActual.toString() + "}";
        insertIntoProceedOrderBook(lastUpdateId, resultJson);
    }

    private void insertOrderBookEventToSourceTable(String eventType, long eventTime, String symbol, long firstUpdateId, long finalUpdateId, JsonNode bids, JsonNode asks) {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            String insertQuery = "INSERT INTO " + SOURCE_ORDER_BOOK_EVENT +
                    " (event_type, event_time, symbol, first_update_id, final_update_id, bids, asks) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                preparedStatement.setString(1, eventType);
                preparedStatement.setLong(2, eventTime);
                preparedStatement.setString(3, symbol);
                preparedStatement.setLong(4, firstUpdateId);
                preparedStatement.setLong(5, finalUpdateId);
                preparedStatement.setObject(6, bids, java.sql.Types.OTHER); // Store "b" (bids) as JSONB
                preparedStatement.setObject(7, asks, java.sql.Types.OTHER); // Store "a" (asks) as JSONB

                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertIntoProceedOrderBook(long lastUpdateId, String partialOrderBook){
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            String insertQuery = "INSERT INTO " + PROCEED_ORDER_BOOK + " (lastUpdateId, partial_order_book) VALUES (?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                preparedStatement.setLong(1, lastUpdateId);
                preparedStatement.setObject(2, partialOrderBook, java.sql.Types.OTHER); // Set the data type explicitly
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void getDepthSnapshot() {
        HttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(DEPTH_API_URL);

        try {
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity);
            if (!responseBody.isEmpty()) {
                OrderBookSnapshot snapshot = parseOrderBookSnapshot(responseBody);
                if (snapshot != null) {
                    // Update the local order book with the bids and asks from the snapshot
                    bidsActual.clear();
                    asksActual.clear();
                    bidsActual.addAll(snapshot.getBids());
                    asksActual.addAll(snapshot.getAsks());

                    // Set the lastUpdateId to the update ID of the snapshot
                    lastUpdateId = snapshot.getLastUpdateId();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Handle exception
        }
    }

    private OrderBookSnapshot parseOrderBookSnapshot(String responseBody) {
        OrderBookSnapshot snapshot = new OrderBookSnapshot();
        JsonParser parser = new JsonParser();

        try {
            // Parse the JSON response body
            JsonObject jsonObject = parser.parse(responseBody).getAsJsonObject();

            // Extract the lastUpdateId
            long lastUpdateId = jsonObject.get("lastUpdateId").getAsLong();
            snapshot.setLastUpdateId(lastUpdateId);

            // Extract the bids and asks arrays
            JsonArray bidsArray = jsonObject.getAsJsonArray("bids");
            JsonArray asksArray = jsonObject.getAsJsonArray("asks");

            // Populate the bids list
            List<OrderBookEvent.PriceQuantityPair> bids = new ArrayList<>();
            for (JsonElement bidElement : bidsArray) {
                JsonArray bidArray = bidElement.getAsJsonArray();
                String price = bidArray.get(0).getAsString();
                String quantity = bidArray.get(1).getAsString();
                OrderBookEvent.PriceQuantityPair bid = new OrderBookEvent.PriceQuantityPair(price, quantity);
                bids.add(bid);
            }

            snapshot.setBids(bids);

            // Populate the asks list
            List<OrderBookEvent.PriceQuantityPair> asks = new ArrayList<>();
            for (JsonElement askElement : asksArray) {
                JsonArray askArray = askElement.getAsJsonArray();
                String price = askArray.get(0).getAsString();
                String quantity = askArray.get(1).getAsString();
                OrderBookEvent.PriceQuantityPair ask = new OrderBookEvent.PriceQuantityPair(price, quantity);
                asks.add(ask);
            }
            snapshot.setAsks(asks);
        } catch (Exception e) {
            System.err.println("Error while parsing order book snapshot: " + e.getMessage());
            return null;
        }

        return snapshot;
    }


    private OrderBookEvent parseOrderBookEvent(String event) {
        try {
            // Parse the JSON event
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(event);

            // Extract the necessary fields
            JsonNode dataNode = jsonNode.get("data");
            String eventType = dataNode.get("e").asText();
            long eventTime = dataNode.get("E").asLong();
            String symbol = dataNode.get("s").asText();
            long firstUpdateId = dataNode.get("U").asLong();
            long finalUpdateId = dataNode.get("u").asLong();

            // Process the bids and asks
            List<OrderBookEvent.PriceQuantityPair> bids = parsePriceQuantityPairs(dataNode.get("b"));
            List<OrderBookEvent.PriceQuantityPair> asks = parsePriceQuantityPairs(dataNode.get("a"));


            // Create and return an OrderBookEvent object
            OrderBookEvent result = new OrderBookEvent(eventType, eventTime, symbol, firstUpdateId, finalUpdateId, bids, asks);
            if (orderBookEventsList.contains(result))
                return null;
            else {
                insertOrderBookEventToSourceTable(eventType, eventTime, symbol, firstUpdateId, finalUpdateId, dataNode.get("b"), dataNode.get("a"));
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null; // Return null in case of any parsing errors
        }
    }

    private List<OrderBookEvent.PriceQuantityPair> parsePriceQuantityPairs(JsonNode node) {
        List<OrderBookEvent.PriceQuantityPair> priceQuantityPairs = new ArrayList<>();
        for (JsonNode pairNode : node) {
            String price = pairNode.get(0).asText();
            String quantity = pairNode.get(1).asText();
            priceQuantityPairs.add(new OrderBookEvent.PriceQuantityPair(price, quantity));
        }
        return priceQuantityPairs;
    }

}
