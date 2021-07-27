package utils;

public class Customer {
    private int customerID;
    private String customerName;
    public Customer(int ID, String Name){
        customerID = ID;
        customerName = Name;
    }

    public int getCustomerID() {
        return customerID;
    }

    public String getCustomerName() {
        return customerName;
    }
}
