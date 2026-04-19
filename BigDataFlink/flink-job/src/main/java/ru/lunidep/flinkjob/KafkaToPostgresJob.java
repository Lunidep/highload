package ru.lunidep.flinkjob;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.lunidep.flinkjob.entity.SaleRecord;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Objects;

public class KafkaToPostgresJob {

    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String KAFKA_GROUP_ID = "flink-postgres-sink";
    private static final String KAFKA_TOPIC = "csv-data-topic";
    
    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/postgres";
    private static final String JDBC_USERNAME = "postgres";
    private static final String JDBC_PASSWORD = "postgres";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");
        
        DataStream<SaleRecord> saleRecords = kafkaStream
                .map((MapFunction<String, SaleRecord>) value -> {
                    try {
                        return objectMapper.readValue(value, SaleRecord.class);
                    } catch (Exception e) {
                        System.err.println("Error parsing JSON: " + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull);
        
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(JDBC_USERNAME)
                .withPassword(JDBC_PASSWORD)
                .build();
        
        saleRecords.addSink(JdbcSink.sink(
                "INSERT INTO customers (first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT DO NOTHING",
                (PreparedStatement ps, SaleRecord record) -> {
                    ps.setString(1, record.getCustomerFirstName());
                    ps.setString(2, record.getCustomerLastName());
                    ps.setInt(3, record.getCustomerAge());
                    ps.setString(4, record.getCustomerEmail());
                    ps.setString(5, record.getCustomerCountry());
                    ps.setString(6, record.getCustomerPostalCode());
                    ps.setString(7, record.getCustomerPetType());
                    ps.setString(8, record.getCustomerPetName());
                    ps.setString(9, record.getCustomerPetBreed());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                jdbcOptions
        )).name("PostgreSQL Sink (Customers)");
        
        saleRecords.addSink(JdbcSink.sink(
                "INSERT INTO sellers (first_name, last_name, email, country, postal_code) " +
                        "VALUES (?, ?, ?, ?, ?) " +
                        "ON CONFLICT DO NOTHING",
                (PreparedStatement ps, SaleRecord record) -> {
                    ps.setString(1, record.getSellerFirstName());
                    ps.setString(2, record.getSellerLastName());
                    ps.setString(3, record.getSellerEmail());
                    ps.setString(4, record.getSellerCountry());
                    ps.setString(5, record.getSellerPostalCode());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                jdbcOptions
        )).name("PostgreSQL Sink (Sellers)");
        
        saleRecords.addSink(JdbcSink.sink(
                "INSERT INTO products (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT DO NOTHING",
                (PreparedStatement ps, SaleRecord record) -> {
                    ps.setString(1, record.getProductName());
                    ps.setString(2, record.getProductCategory());
                    ps.setDouble(3, record.getProductPrice());
                    ps.setObject(4, record.getProductWeight(), Types.DOUBLE);
                    ps.setString(5, record.getProductColor());
                    ps.setString(6, record.getProductSize());
                    ps.setString(7, record.getProductBrand());
                    ps.setString(8, record.getProductMaterial());
                    ps.setString(9, record.getProductDescription());
                    ps.setObject(10, record.getProductRating(), Types.DOUBLE);
                    ps.setObject(11, record.getProductReviews(), Types.INTEGER);
                    ps.setDate(12, record.getProductReleaseDate() != null ? java.sql.Date.valueOf(record.getProductReleaseDate()) : null);
                    ps.setDate(13, record.getProductExpiryDate() != null ? java.sql.Date.valueOf(record.getProductExpiryDate()) : null);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                jdbcOptions
        )).name("PostgreSQL Sink (Products)");
        
        saleRecords.addSink(JdbcSink.sink(
                "INSERT INTO stores (name, location, city, state, country, phone, email) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT DO NOTHING",
                (PreparedStatement ps, SaleRecord record) -> {
                    ps.setString(1, record.getStoreName());
                    ps.setString(2, record.getStoreLocation());
                    ps.setString(3, record.getStoreCity());
                    ps.setString(4, record.getStoreState());
                    ps.setString(5, record.getStoreCountry());
                    ps.setString(6, record.getStorePhone());
                    ps.setString(7, record.getStoreEmail());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                jdbcOptions
        )).name("PostgreSQL Sink (Stores)");
        
        saleRecords.addSink(JdbcSink.sink(
                "INSERT INTO suppliers (name, contact, email, phone, address, city, country) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT DO NOTHING",
                (PreparedStatement ps, SaleRecord record) -> {
                    ps.setString(1, record.getSupplierName());
                    ps.setString(2, record.getSupplierContact());
                    ps.setString(3, record.getSupplierEmail());
                    ps.setString(4, record.getSupplierPhone());
                    ps.setString(5, record.getSupplierAddress());
                    ps.setString(6, record.getSupplierCity());
                    ps.setString(7, record.getSupplierCountry());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                jdbcOptions
        )).name("PostgreSQL Sink (Suppliers)");
        
        saleRecords.addSink(JdbcSink.sink(
                "INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price) " +
                        "SELECT c.customer_id, s.seller_id, p.product_id, st.store_id, su.supplier_id, ?, ?, ? " +
                        "FROM (SELECT 1) AS dummy " +
                        "LEFT JOIN customers c ON c.email = ? " +
                        "LEFT JOIN sellers s ON s.email = ? " +
                        "LEFT JOIN products p ON p.name = ? AND p.brand = ? " +
                        "LEFT JOIN stores st ON st.name = ? AND st.location = ? " +
                        "LEFT JOIN suppliers su ON su.name = ? AND su.email = ? " +
                        "ON CONFLICT DO NOTHING",
                (PreparedStatement ps, SaleRecord record) -> {
                    ps.setDate(1, java.sql.Date.valueOf(record.getSaleDate()));
                    ps.setInt(2, record.getSaleQuantity());
                    ps.setDouble(3, record.getSaleTotalPrice());
                    ps.setString(4, record.getCustomerEmail());
                    ps.setString(5, record.getSellerEmail());
                    ps.setString(6, record.getProductName());
                    ps.setString(7, record.getProductBrand());
                    ps.setString(8, record.getStoreName());
                    ps.setString(9, record.getStoreLocation());
                    ps.setString(10, record.getSupplierName());
                    ps.setString(11, record.getSupplierEmail());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                jdbcOptions
        )).name("PostgreSQL Sink (Fact Sales)");
        
        env.execute("Kafka to PostgreSQL Data Pipeline");
    }
}
