package com.github.guilhermealves.avro;

import org.apache.avro.reflect.Nullable;

public class MyReflectedCustomer {

    private String firstName;
    private String lastName;
    @Nullable private String nickName;

    public MyReflectedCustomer (){}

    public MyReflectedCustomer (String firstName, String lastName, String nickName) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.nickName = nickName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String fullName(){
        return String.format("%s %s %s", firstName, lastName, nickName);
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }
}
