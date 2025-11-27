package ru.lunidep.kafkaproducer.models;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;

@Data
public class CustomerData {

    @CsvBindByName(column = "id")
    private Long id;

    @CsvBindByName(column = "customer_first_name")
    private String customerFirstName;

    @CsvBindByName(column = "customer_last_name")
    private String customerLastName;

    @CsvBindByName(column = "customer_age")
    private Integer customerAge;

    @CsvBindByName(column = "customer_email")
    private String customerEmail;

    @CsvBindByName(column = "customer_country")
    private String customerCountry;

    @CsvBindByName(column = "customer_postal_code")
    private String customerPostalCode;

    @CsvBindByName(column = "customer_pet_type")
    private String customerPetType;

    @CsvBindByName(column = "customer_pet_name")
    private String customerPetName;

    @CsvBindByName(column = "customer_pet_breed")
    private String customerPetBreed;

    @CsvBindByName(column = "seller_first_name")
    private String sellerFirstName;

    @CsvBindByName(column = "seller_last_name")
    private String sellerLastName;

    @CsvBindByName(column = "seller_email")
    private String sellerEmail;

    @CsvBindByName(column = "seller_country")
    private String sellerCountry;

    @CsvBindByName(column = "seller_postal_code")
    private String sellerPostalCode;

    @CsvBindByName(column = "product_name")
    private String productName;

    @CsvBindByName(column = "product_category")
    private String productCategory;

    @CsvBindByName(column = "product_price")
    private Double productPrice;

    @CsvBindByName(column = "product_quantity")
    private Integer productQuantity;

    @CsvBindByName(column = "sale_date")
    private String saleDate;

    @CsvBindByName(column = "sale_customer_id")
    private Long saleCustomerId;

    @CsvBindByName(column = "sale_seller_id")
    private Long saleSellerId;

    @CsvBindByName(column = "sale_product_id")
    private Long saleProductId;

    @CsvBindByName(column = "sale_quantity")
    private Integer saleQuantity;

    @CsvBindByName(column = "sale_total_price")
    private Double saleTotalPrice;

    @CsvBindByName(column = "store_name")
    private String storeName;

    @CsvBindByName(column = "store_location")
    private String storeLocation;

    @CsvBindByName(column = "store_city")
    private String storeCity;

    @CsvBindByName(column = "store_state")
    private String storeState;

    @CsvBindByName(column = "store_country")
    private String storeCountry;

    @CsvBindByName(column = "store_phone")
    private String storePhone;

    @CsvBindByName(column = "store_email")
    private String storeEmail;

    @CsvBindByName(column = "pet_category")
    private String petCategory;

    @CsvBindByName(column = "product_weight")
    private Double productWeight;

    @CsvBindByName(column = "product_color")
    private String productColor;

    @CsvBindByName(column = "product_size")
    private String productSize;

    @CsvBindByName(column = "product_brand")
    private String productBrand;

    @CsvBindByName(column = "product_material")
    private String productMaterial;

    @CsvBindByName(column = "product_description")
    private String productDescription;

    @CsvBindByName(column = "product_rating")
    private Double productRating;

    @CsvBindByName(column = "product_reviews")
    private Integer productReviews;

    @CsvBindByName(column = "product_release_date")
    private String productReleaseDate;

    @CsvBindByName(column = "product_expiry_date")
    private String productExpiryDate;

    @CsvBindByName(column = "supplier_name")
    private String supplierName;

    @CsvBindByName(column = "supplier_contact")
    private String supplierContact;

    @CsvBindByName(column = "supplier_email")
    private String supplierEmail;

    @CsvBindByName(column = "supplier_phone")
    private String supplierPhone;

    @CsvBindByName(column = "supplier_address")
    private String supplierAddress;

    @CsvBindByName(column = "supplier_city")
    private String supplierCity;

    @CsvBindByName(column = "supplier_country")
    private String supplierCountry;
}