package com.performance.domain.entity;

public class UserMaster {

    private Long id;
    private String lastName;
    private String firstName;
    private String prefectures;
    private String city;
    private String bloodType;
    private String hobby1;
    private String hobby2;
    private String hobby3;
    private String hobby4;
    private String hobby5;

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
    public String getHobby1() {
        return hobby1;
    }
    public void setHobby1(String hobby1) {
        this.hobby1 = hobby1;
    }
    public String getHobby2() {
        return hobby2;
    }
    public void setHobby2(String hobby2) {
        this.hobby2 = hobby2;
    }
    public String getHobby3() {
        return hobby3;
    }
    public void setHobby3(String hobby3) {
        this.hobby3 = hobby3;
    }
    public String getHobby4() {
        return hobby4;
    }
    public void setHobby4(String hobby4) {
        this.hobby4 = hobby4;
    }
    public String getHobby5() {
        return hobby5;
    }
    public void setHobby5(String hobby5) {
        this.hobby5 = hobby5;
    }
    
    public String toString() {
        
        return lastName + firstName + prefectures + city + bloodType + hobby1 + hobby2 + hobby3 + hobby4 + hobby5;
    }
}
