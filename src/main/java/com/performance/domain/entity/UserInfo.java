package com.performance.domain.entity;

public class UserInfo {

    private Long id;
    private String lastName;
    private String firstName;
    private String prefectures;
    private String city;
    private String bloodType;

    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public String getLastName() {
        return lastName;
    }
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    public String getFirstName() {
        return firstName;
    }
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    public String getPrefectures() {
        return prefectures;
    }
    public void setPrefectures(String prefectures) {
        this.prefectures = prefectures;
    }
    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
    public String getBloodType() {
        return bloodType;
    }
    public void setBloodType(String bloodType) {
        this.bloodType = bloodType;
    }
    
    public String toString() {
        
        return lastName + firstName + prefectures + city + bloodType;
    }
}
