package com.github.guilhermealves.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class MyReflectionExamples {

    public static void main(String[] args) {

        Schema schema = ReflectData.get().getSchema(MyReflectedCustomer.class);
        System.out.println("schema = " + schema.toString(true));

        System.out.println("Writing customer-reflected.avro");
        File file = new File("customer-reflected.avro");
        final DatumWriter<MyReflectedCustomer> datumWriter = new ReflectDatumWriter<>(MyReflectedCustomer.class);
        try (DataFileWriter<MyReflectedCustomer> writer = new DataFileWriter<>(datumWriter)) {
            writer.setCodec(CodecFactory.deflateCodec(9))
                    .create(schema, file)
                    .append(new MyReflectedCustomer("Bill", "Clark", "The Rocket"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Reading customer-reflected.avro");
        final DatumReader<MyReflectedCustomer> datumReader = new ReflectDatumReader<>(MyReflectedCustomer.class);
        try (DataFileReader<MyReflectedCustomer> reader = new DataFileReader<>(file, datumReader)) {
            for (MyReflectedCustomer myReflectedCustomer : reader) {
                System.out.println(myReflectedCustomer.fullName());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
