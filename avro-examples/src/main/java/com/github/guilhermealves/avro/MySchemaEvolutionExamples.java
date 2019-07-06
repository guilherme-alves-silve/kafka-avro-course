package com.github.guilhermealves.avro;

import java.io.File;
import java.io.IOException;

import com.example.CustomerV1;
import com.example.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class MySchemaEvolutionExamples {

    public static void main (String[] args) {

        CustomerV1 customerV1 = CustomerV1.newBuilder()
                                    .setAge(34)
                                    .setAutomatedEmail(false)
                                    .setFirstName("John")
                                    .setLastName("Doe")
                                    .setHeight(178f)
                                    .setWeight(75f)
                                    .build();
        System.out.println("Customer V1 = " + customerV1);

        final File customerV1Path = new File("customerV1.avro");
        final DatumWriter<CustomerV1> datumWriterV1 = new SpecificDatumWriter<>(CustomerV1.class);
        try (DataFileWriter<CustomerV1> writer = new DataFileWriter<>(datumWriterV1)) {
            writer.create(customerV1.getSchema(), customerV1Path);
            writer.append(customerV1);
            System.out.println("Successfully wrote customerV1.avro");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Reading our customerV1.avro with v2 schema");
        final DatumReader<CustomerV2> datumReaderV2 = new SpecificDatumReader<>(CustomerV2.class);
        try (DataFileReader<CustomerV2> reader = new DataFileReader<>(customerV1Path, datumReaderV2)) {
            while (reader.hasNext()) {
                CustomerV2 customerV2 = reader.next();
                System.out.println("Customer V2 = " + customerV2);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Backward schema evolution successfull\n\n\n");

        CustomerV2 customerV2 = CustomerV2.newBuilder()
                                .setAge(25)
                                .setFirstName("Mark")
                                .setLastName("Simpson")
                                .setEmail("mark.simpson@gmail.com")
                                .setHeight(160f)
                                .setWeight(65f)
                                .setPhoneNumber("123-456-7890")
                                .build();

        System.out.println("Customer V2 = " + customerV2);

        final File customerV2Path = new File("customerV2.avro");
        final DatumWriter<CustomerV2> datumWriterV2 = new SpecificDatumWriter<>(CustomerV2.class);
        try (DataFileWriter<CustomerV2> writer = new DataFileWriter<>(datumWriterV2)) {
            writer.create(customerV2.getSchema(), customerV2Path);
            writer.append(customerV2);
            System.out.println("Successfully wrote customerV2.avro");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Reading our customerV2.avro with v1 schema");
        final DatumReader<CustomerV1> datumReaderV1 = new SpecificDatumReader<>(CustomerV1.class);
        try (DataFileReader<CustomerV1> reader = new DataFileReader<>(customerV2Path, datumReaderV1)) {
            while (reader.hasNext()) {
                CustomerV1 customerV1Read = reader.next();
                System.out.println("Customer V1 = " + customerV1Read);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Backward schema evolution successfull\n\n\n");
    }
}
