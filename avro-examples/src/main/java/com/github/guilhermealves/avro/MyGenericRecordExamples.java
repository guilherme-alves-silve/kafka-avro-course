package com.github.guilhermealves.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class MyGenericRecordExamples {

    public static void main (String[] args) {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n"
                + "     \"type\": \"record\",\n"
                + "     \"namespace\": \"com.example\",\n"
                + "     \"name\": \"Customer\",\n"
                + "     \"doc\": \"Avro Schema for our Customer\",     \n"
                + "     \"fields\": [\n"
                + "       { \"name\": \"firstName\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n"
                + "       { \"name\": \"lastName\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n"
                + "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n"
                + "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n"
                + "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n"
                + "       { \"name\": \"automatedEmail\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n"
                + "     ]\n"
                + "}");

        GenericData.Record customer = new GenericRecordBuilder(schema)
                .set("firstName", "John")
                .set("lastName", "Doe")
                .set("age", 25)
                .set("height", 170f)
                .set("weight", 80.5f)
                .set("automatedEmail", false)
                .build();

        System.out.println(customer);

        System.out.println("***********************************");

        GenericData.Record customerDefault = new GenericRecordBuilder(schema)
                .set("firstName", "John")
                .set("lastName", "Doe")
                .set("age", 25)
                .set("height", 170f)
                .set("weight", 80.5f)
                .build();

        System.out.println(customerDefault);

        System.out.println("***********************************");

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
            writer.create(customer.getSchema(), new File("customer-generic.avro"));
            writer.append(customer);
            writer.append(customerDefault);
            System.out.println("Written customer-generic.avro");
        } catch (IOException ex) {
            System.out.println("Couldn't write file");
            ex.printStackTrace();
        }

        System.out.println("***********************************");

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(
                new File("customer-generic.avro"), datumReader)) {

            while (reader.hasNext()) {
                GenericRecord readCustomer = reader.next();
                System.out.println("Read customer-generic.avro");
                System.out.println(readCustomer);

                System.out.println("First name: " + readCustomer.get("firstName"));
                System.out.println("Non existent field: " + readCustomer.get("not_here"));
            }
        } catch (IOException ex) {
            System.out.println("Couldn't read file");
            ex.printStackTrace();
        }

        System.out.println("***********************************");
    }

}
