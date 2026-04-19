package ru.lunidep.flinkjob.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SaleRecord {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("customerFirstName")
    private String customerFirstName;

    @JsonProperty("customerLastName")
    private String customerLastName;

    @JsonProperty("customerAge")
    private Integer customerAge;

    @JsonProperty("customerEmail")
    private String customerEmail;

    @JsonProperty("customerCountry")
    private String customerCountry;

    @JsonProperty("customerPostalCode")
    private String customerPostalCode;

    @JsonProperty("customerPetType")
    private String customerPetType;

    @JsonProperty("customerPetName")
    private String customerPetName;

    @JsonProperty("customerPetBreed")
    private String customerPetBreed;

    @JsonProperty("sellerFirstName")
    private String sellerFirstName;

    @JsonProperty("sellerLastName")
    private String sellerLastName;

    @JsonProperty("sellerEmail")
    private String sellerEmail;

    @JsonProperty("sellerCountry")
    private String sellerCountry;

    @JsonProperty("sellerPostalCode")
    private String sellerPostalCode;

    @JsonProperty("productName")
    private String productName;

    @JsonProperty("productCategory")
    private String productCategory;

    @JsonProperty("productPrice")
    private Double productPrice;

    @JsonProperty("productQuantity")
    private Integer productQuantity;

    @JsonProperty("saleDate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "M/d/yyyy")
    private LocalDate saleDate;

    @JsonProperty("saleCustomerId")
    private Long saleCustomerId;

    @JsonProperty("saleSellerId")
    private Long saleSellerId;

    @JsonProperty("saleProductId")
    private Long saleProductId;

    @JsonProperty("saleQuantity")
    private Integer saleQuantity;

    @JsonProperty("saleTotalPrice")
    private Double saleTotalPrice;

    @JsonProperty("storeName")
    private String storeName;

    @JsonProperty("storeLocation")
    private String storeLocation;

    @JsonProperty("storeCity")
    private String storeCity;

    @JsonProperty("storeState")
    private String storeState;

    @JsonProperty("storeCountry")
    private String storeCountry;

    @JsonProperty("storePhone")
    private String storePhone;

    @JsonProperty("storeEmail")
    private String storeEmail;

    @JsonProperty("petCategory")
    private String petCategory;

    @JsonProperty("productWeight")
    private Double productWeight;

    @JsonProperty("productColor")
    private String productColor;

    @JsonProperty("productSize")
    private String productSize;

    @JsonProperty("productBrand")
    private String productBrand;

    @JsonProperty("productMaterial")
    private String productMaterial;

    @JsonProperty("productDescription")
    private String productDescription;

    @JsonProperty("productRating")
    private Double productRating;

    @JsonProperty("productReviews")
    private Integer productReviews;

    @JsonProperty("productReleaseDate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "M/d/yyyy")
    private LocalDate productReleaseDate;

    @JsonProperty("productExpiryDate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "M/d/yyyy")
    private LocalDate productExpiryDate;

    @JsonProperty("supplierName")
    private String supplierName;

    @JsonProperty("supplierContact")
    private String supplierContact;

    @JsonProperty("supplierEmail")
    private String supplierEmail;

    @JsonProperty("supplierPhone")
    private String supplierPhone;

    @JsonProperty("supplierAddress")
    private String supplierAddress;

    @JsonProperty("supplierCity")
    private String supplierCity;

    @JsonProperty("supplierCountry")
    private String supplierCountry;
}
