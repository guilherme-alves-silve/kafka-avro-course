package com.github.guilhermealves.avro;

import java.io.File;
import java.io.IOException;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class MySpecificRecordExamples {

    public static void main(String[] args) {

        Customer.Builder customerBuilder = Customer.newBuilder()
                .setAge(25)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(175.5f)
                .setWeight(80.5f)
                .setAutomatedEmail(false);
        Customer customer = customerBuilder.build();
        System.out.println(customer);

        DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> writer = new DataFileWriter<>(datumWriter)) {
            writer.create(customer.getSchema(), new File("customer-specific.avro"));
            writer.append(customer);
            System.out.println("Successfully wrote customer-specific.avro");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        File file = new File("customer-specific.avro");
        DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try (DataFileReader<Customer> reader = new DataFileReader<>(file, datumReader)) {
            System.out.println("Reading our specific record");
            while (reader.hasNext()) {
                Customer readCustomer = reader.next();
                System.out.println(readCustomer);
                System.out.println("First name: " + readCustomer.getFirstName());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
