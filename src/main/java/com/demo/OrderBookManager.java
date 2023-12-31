package com.demo;

import com.binance.connector.client.WebSocketStreamClient;
import com.binance.connector.client.impl.WebSocketStreamClientImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.*;
import org.postgresql.util.PSQLException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class OrderBookManager {

    private Timer timer;
    private String SOURCE_ORDER_BOOK_EVENT;
    private String SOURCE_SHAPSHOT_BOOK;
    private String DEPTH_API_URL;
    private DatabaseConfig databaseConfig;
    private AppConfig appConfig;

    private Boolean startTracking = true;

    private MessageSenderService messageSenderService;

    public void setMessageSenderService(MessageSenderService messageSenderService) {
        this.messageSenderService = messageSenderService;
    }


    private RabbitTemplate rabbitTemplate;


    public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void createNewInstance(DatabaseConfig databaseConfig, AppConfig appConfig){
        this.databaseConfig = databaseConfig;
        this.appConfig = appConfig;
        findLastCreatedTable();
    }


    public void startOrderBookManage(DatabaseConfig databaseConfig, AppConfig appConfig, boolean update_parameter) {
        this.databaseConfig = databaseConfig;
        this.appConfig = appConfig;
        DEPTH_API_URL = "https://api.binance.com/api/v3/depth?symbol=" + appConfig.getEventSymbol().toUpperCase(Locale.ROOT) + "&limit=" + appConfig.getLimitCount();
        if (!update_parameter) {
            SOURCE_ORDER_BOOK_EVENT = "source_order_book_event_" + appConfig.getEventSymbol() + "_";
            SOURCE_SHAPSHOT_BOOK = "source_snapshot_book_" + appConfig.getEventSymbol() + "_";
            createSourceTable();
            createSourceShapshotTable();
        }
        else{
            startTracking = true;
            findLastCreatedTable();
        }
        startOrderBookEventStream();
    }

    public void findLastCreatedTable() {
        String sourcePattern = "^source_order_book_event_" + appConfig.getEventSymbol() + "_([0-9]+)$";
        String proceedPattern = "^source_snapshot_book_" + appConfig.getEventSymbol() + "_([0-9]+)$";
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

        SOURCE_ORDER_BOOK_EVENT = maxSourceTableName;
        SOURCE_SHAPSHOT_BOOK = maxProceedTableName;
    }

    private void startOrderBookEventStream(){
        WebSocketStreamClient wsStreamClient = new WebSocketStreamClientImpl();
        ArrayList<String> streams = new ArrayList<>();
        streams.add(appConfig.getEventSymbol() + "@depth@" + appConfig.getUpdateSpeed() + "ms");

        wsStreamClient.combineStreams(streams, (event) -> {
            OrderBookEvent orderBookEvent = parseOrderBookEvent(event);
            if (orderBookEvent != null) {
                if (startTracking ) {
                    String orderBookSnapshot = getDepthSnapshot();
                    if (orderBookSnapshot != null) {
                        insertIntoSourceSnapshot(orderBookSnapshot, Instant.now().toEpochMilli());
                    }
                    startTracking = false;
                    startTimer();
                }

            } else {
                // Handle parsing errors
                System.err.println("Failed order book event or doubled in cached list: " + event);
            }
        });

        // Schedule the WebSocket renewal before it expires
        scheduleWebSocketRenewal();
    }

    private long calculateDelayToNextHour() {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime now = LocalDateTime.now(zoneId);
        int minutesUntilNextHour = 60 - now.getMinute();

        return minutesUntilNextHour * 60 * 1000L; // Convert minutes to milliseconds
    }

    private void startTimer(){
        if (timer != null) {
            timer.cancel();
        }

        timer = new Timer();
        long delay = calculateDelayToNextHour();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                String orderBookSnapshot = getDepthSnapshot();
                if (orderBookSnapshot != null) {
                    insertIntoSourceSnapshot(orderBookSnapshot, Instant.now().toEpochMilli());
                }
            }
        }, delay, 60 * 60 * 1000); // Schedule to run every one hour (60 minutes * 60 seconds * 1000 milliseconds)
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

    private void createSourceShapshotTable(){
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            // Generate the table name with the current time in Unix format
            long currentTimeUnix = System.currentTimeMillis() / 1000L;
            SOURCE_SHAPSHOT_BOOK = SOURCE_SHAPSHOT_BOOK + currentTimeUnix;

            String createQuery = "CREATE TABLE " + SOURCE_SHAPSHOT_BOOK + " (" +
                    "currentTime BIGINT, " +
                    "partial_book JSONB " +
                    ")";

            try (PreparedStatement statement = connection.prepareStatement(createQuery)) {
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertIntoSourceSnapshot(String orderBookSnapshot, long currentTimeUnix){
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            String insertQuery = "INSERT INTO " + SOURCE_SHAPSHOT_BOOK + " (currentTime, partial_book) VALUES (?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                preparedStatement.setLong(1, currentTimeUnix);
                preparedStatement.setObject(2, orderBookSnapshot, java.sql.Types.OTHER);
                preparedStatement.executeUpdate();
            }
        } catch (PSQLException e) {
            //nothing
        } catch (SQLException e) {
            //nothing
        }

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
                    "final_update_id BIGINT PRIMARY KEY, " + // Added PRIMARY KEY constraint for final_update_id
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
        } catch (PSQLException e) {
            //nothing
        } catch (SQLException e) {
            //nothing
        }
    }


    private String getDepthSnapshot() {
        HttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(DEPTH_API_URL);

        try {
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            String responseBody = EntityUtils.toString(entity);
            if (!responseBody.isEmpty()) {
                return responseBody;
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Handle exception
        }
        return null;
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
            insertOrderBookEventToSourceTable(eventType, eventTime, symbol, firstUpdateId, finalUpdateId, dataNode.get("b"), dataNode.get("a"));
            return result;
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

    public Optional<OrderBookSnapshot> findClosestSnapshot(long currentTime) {
        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            String selectQuery = "SELECT * FROM " + SOURCE_SHAPSHOT_BOOK +
                    " WHERE currentTime <= ? ORDER BY currentTime DESC LIMIT 1";

            try (PreparedStatement statement = connection.prepareStatement(selectQuery)) {
                statement.setLong(1, currentTime);

                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        long currentTimeDB = resultSet.getLong("currentTime");
                        String partial_bookJson = resultSet.getString("partial_book");

                        // Assuming you have a method to parse JSON strings and create the OrderBookSnapshot object
                        OrderBookSnapshot snapshot = parseOrderBookSnapshot(partial_bookJson);
                        snapshot.setCurrentTime(currentTimeDB);
                        return Optional.of(snapshot);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Optional.empty();
    }

    public List<OrderBookEvent> getRowsBetweenTimes(long time1, long time2) throws JsonProcessingException {
        List<OrderBookEvent> result = new ArrayList<>();

        try (Connection connection = DriverManager.getConnection(databaseConfig.getDbUrl(), databaseConfig.getDbUsername(), databaseConfig.getDbPassword())) {
            String selectQuery = "SELECT * FROM " + SOURCE_ORDER_BOOK_EVENT +
                    " WHERE event_time >= ? AND event_time <= ?";

            try (PreparedStatement statement = connection.prepareStatement(selectQuery)) {
                statement.setLong(1, time1);
                statement.setLong(2, time2);

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        OrderBookEvent event = mapResultSetToOrderBookEvent(resultSet);
                        result.add(event);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    private OrderBookEvent mapResultSetToOrderBookEvent(ResultSet resultSet) throws SQLException, JsonProcessingException {
        // Implement a method to map the ResultSet to an OrderBookEvent object
        // Example:
        String eventType = resultSet.getString("event_type");
        long eventTime = resultSet.getLong("event_time");
        String symbol = resultSet.getString("symbol");
        long firstUpdateId = resultSet.getLong("first_update_id");
        long finalUpdateId = resultSet.getLong("final_update_id");
        String bidsJson = resultSet.getString("bids");
        List<OrderBookEvent.PriceQuantityPair> bids = parseBidsAsks(bidsJson);
        String asksJson = resultSet.getString("asks");
        List<OrderBookEvent.PriceQuantityPair> asks = parseBidsAsks(asksJson);

        // Assuming you have a method to parse JSON strings and create the OrderBookEvent object
        OrderBookEvent event = new OrderBookEvent(eventType, eventTime, symbol, firstUpdateId, finalUpdateId, bids, asks);
        return event;
    }

    public static List<OrderBookEvent.PriceQuantityPair> parseBidsAsks(String bidsJson) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<List<String>> parsedBids = objectMapper.readValue(bidsJson, List.class);

        List<OrderBookEvent.PriceQuantityPair> bids = new ArrayList<>();

        for (List<String> bid : parsedBids) {
            if (bid.size() >= 2) {
                OrderBookEvent.PriceQuantityPair priceQuantityPair = new OrderBookEvent.PriceQuantityPair(bid.get(0), bid.get(1));
                bids.add(priceQuantityPair);
            }
        }

        return bids;
    }

    private static List<OrderBookEvent.PriceQuantityPair> getAllBids(List<OrderBookEvent> orderBookEvents) {
        List<OrderBookEvent.PriceQuantityPair> allBids = new ArrayList<>();

        for (OrderBookEvent event : orderBookEvents) {
            if (event.getBids() != null) {
                allBids.addAll(event.getBids());
            }
        }

        return allBids;
    }

    public static List<OrderBookEvent.PriceQuantityPair> getAllAsks(List<OrderBookEvent> orderBookEvents) {
        List<OrderBookEvent.PriceQuantityPair> allAsks = new ArrayList<>();

        for (OrderBookEvent event : orderBookEvents) {
            if (event.getAsks() != null) {
                allAsks.addAll(event.getAsks());
            }
        }

        return allAsks;
    }

    private OrderBookSnapshot processBidsAndAsks(List<OrderBookEvent.PriceQuantityPair> bids, List<OrderBookEvent.PriceQuantityPair> asks) {
      for (int i = 0; i < bids.size(); i++){
            OrderBookEvent.PriceQuantityPair bid = bids.get(i);
            String bidPrice = bid.getPrice();
            String bidQuantity = bid.getQuantity();
            for (int j = 0; j < asks.size(); j++){
                OrderBookEvent.PriceQuantityPair ask = asks.get(j);
                String askPrice = ask.getPrice();
                String askQuantity = ask.getQuantity();
                if (bidPrice.equals(askPrice) && bidQuantity.equals(askQuantity)) {
                    // Delete both the bid and ask
                    bids.remove(i);
                    asks.remove(j);
                    break;
                } else if (bidPrice.equals(askPrice)) {
                    double bidQuantityValue = Double.parseDouble(bidQuantity);
                    double askQuantityValue = Double.parseDouble(askQuantity);

                    if (bidQuantityValue > askQuantityValue) {
                        // Update bid quantity and delete the ask
                        bidQuantityValue -= askQuantityValue;
                        bid.setQuantity(String.valueOf(bidQuantityValue));
                        try {
                            asks.remove(j);
                        }
                        catch (IndexOutOfBoundsException exception){
                            //nothing
                        }
                    } else {
                        // Update ask quantity and delete the bid
                        askQuantityValue -= bidQuantityValue;
                        ask.setQuantity(String.valueOf(askQuantityValue));
                        try {
                            bids.remove(i);
                        }
                        catch (IndexOutOfBoundsException exception){
                            //nothing
                        }
                    }
                }
            }
        }
        OrderBookSnapshot result = new OrderBookSnapshot();
        result.setBids(bids);
        result.setAsks(asks);
        return result;
    }


    private OrderBookSnapshot accumulateSnapshotActualBids(List<OrderBookEvent> orderBookEvents, OrderBookSnapshot orderBookSnapshot){
        List<OrderBookEvent.PriceQuantityPair> bidsFromShapshot = orderBookSnapshot.getBids();
        List<OrderBookEvent.PriceQuantityPair> asksFromShapshot = orderBookSnapshot.getAsks();
        long lastUpdatedId = 0;
        if (orderBookEvents.size() > 0) {
            int lastIndex = orderBookEvents.size() - 1;
            lastUpdatedId = orderBookEvents.get(lastIndex).getFinalUpdateId();
        }
        for (OrderBookEvent.PriceQuantityPair pair: bidsFromShapshot){
            for (OrderBookEvent.PriceQuantityPair pairActual: getAllBids(orderBookEvents)){
                if (Double.valueOf(pair.getPrice()).equals(Double.valueOf(pairActual.getPrice())))
                    pair.setQuantity(pairActual.getQuantity());
            }

        }
        for (OrderBookEvent.PriceQuantityPair pairActual: getAllBids(orderBookEvents)){
            if (!bidsFromShapshot.stream().anyMatch(x -> Double.valueOf(x.getPrice()).equals(Double.valueOf(pairActual.getPrice()))))
                bidsFromShapshot.add(pairActual);
            if (Double.valueOf(pairActual.getQuantity()).equals(0.0))
                bidsFromShapshot.remove(pairActual);
        }

        for (OrderBookEvent.PriceQuantityPair pair: asksFromShapshot){
            for (OrderBookEvent.PriceQuantityPair pairActual: getAllAsks(orderBookEvents)){
                if (Double.valueOf(pair.getPrice()).equals(Double.valueOf(pairActual.getPrice())))
                    pair.setQuantity(pairActual.getQuantity());
            }
        }
        for (OrderBookEvent.PriceQuantityPair pairActual: getAllAsks(orderBookEvents)){
            if (!asksFromShapshot.stream().anyMatch(x -> Double.valueOf(x.getPrice()).equals(Double.valueOf(pairActual.getPrice()))))
                asksFromShapshot.add(pairActual);
            if (Double.valueOf(pairActual.getQuantity()).equals(0.0))
                asksFromShapshot.remove(pairActual);
        }

        Collections.sort(bidsFromShapshot, new Comparator<OrderBookEvent.PriceQuantityPair>() {
            public int compare(OrderBookEvent.PriceQuantityPair p1, OrderBookEvent.PriceQuantityPair p2) {
                return Double.compare(Double.valueOf(p2.getPrice()), Double.valueOf(p1.getPrice())); // Сортировка по убыванию цены
            }

        });

        Collections.sort(asksFromShapshot, new Comparator<OrderBookEvent.PriceQuantityPair>() {
            public int compare(OrderBookEvent.PriceQuantityPair p1, OrderBookEvent.PriceQuantityPair p2) {
                return Double.compare(Double.valueOf(p1.getPrice()), Double.valueOf(p2.getPrice()));
            }

        });

        OrderBookSnapshot result = processBidsAndAsks(bidsFromShapshot, asksFromShapshot);
        result.setLastUpdateId(lastUpdatedId);
        return result;
    }


    public OrderBookSnapshot collectData(long currentTime) throws JsonProcessingException {
        Optional<OrderBookSnapshot> orderBookSnapshot = findClosestSnapshot(currentTime);
        List<OrderBookEvent> orderBookEvents = getRowsBetweenTimes(orderBookSnapshot.get().getCurrentTime(), currentTime);
        if (orderBookSnapshot.get() != null) {
            return accumulateSnapshotActualBids(orderBookEvents, orderBookSnapshot.get());
        }
        else {
            OrderBookSnapshot orderBookSnapshot1 = processBidsAndAsks(getAllBids(orderBookEvents), getAllAsks(orderBookEvents));
            int lastIndex = orderBookEvents.size() - 1;
            orderBookSnapshot1.setLastUpdateId(orderBookEvents.get(lastIndex).getFinalUpdateId());
            return orderBookSnapshot1;
        }
    }


    public void startWritingTemporaryBook(long startTime,
                                          long endTime,
                                          int intervalMinutes,
                                          String name_queue) throws JsonProcessingException {
        Optional<OrderBookSnapshot> snapshot = findClosestSnapshot(startTime);
        OrderBookSnapshot snapshotCurrent = null;
        if (snapshot != null)
            snapshotCurrent = snapshot.get();
        messageSenderService.createQueue(name_queue);
        while (startTime < endTime) {
            List<OrderBookEvent> orderBookEvents = getRowsBetweenTimes(snapshotCurrent.getCurrentTime(), startTime + intervalMinutes);
            OrderBookSnapshot temp = accumulateSnapshotActualBids(orderBookEvents, snapshotCurrent);
            // Prepare the JSON object
            Gson gson = new Gson();
            String json = gson.toJson(temp);
            messageSenderService.sendMessage(name_queue, json);
            snapshotCurrent = temp;
            startTime += intervalMinutes;
        }

    }

    public String getTemporaryOrderBook(String name_queue) {
        try {
            // Retrieve a single message from the queue as a ParameterizedTypeReference
            String oneMessage = String.valueOf(rabbitTemplate.receiveAndConvert(name_queue));

            if (oneMessage != null) {
                return oneMessage;
            } else {
                // If the queue is empty, you can return a default value or handle it as needed
                return null;
            }
        } catch (Exception e) {
            // Handle exceptions as needed (e.g., log or throw)
            e.printStackTrace();
            return null;
        }
    }

}
