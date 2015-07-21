/**
 * Copyright (C) 2014-2015 SINTEF
 *
 *     Brian Elvesæter <brian.elvesater@sintef.no>
 *     Shahzad Karamat <shazad.karamat@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.modelbased.proasense.adapter.mhwirth;

import eu.proasense.internal.ComplexValue;
import eu.proasense.internal.SimpleEvent;
import eu.proasense.internal.VariableType;
import net.modelbased.proasense.adapter.base.AbstractBaseAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.joda.time.DateTime;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class MyBaseAdapter extends AbstractBaseAdapter {
    private Properties adapterProperties;
    private KafkaProducer<String, byte[]> producer;


    public MyBaseAdapter() {
        // Write code here that:
        // 1. Reads data from the input port (not provided in the base net.modelbased.proasense.adapter.base)
        String input = "{\"variable_type\":\"1002311\",\"value\":138.2346,\"variable_timestamp\":\"2014-02-14T07:01:24.133Z\"}";
        // 2. Converts read data to simple events
        SimpleEvent event = convertToSimpleEvent(input);
        // 3. Publishes simple events to the output port
        this.outputPort.publishSimpleEvent(event);
    }


    Properties loadAdapterProperties() {
        adapterProperties = new Properties();
        String propFilename = "adapter.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFilename);

        try {
            if (inputStream != null) {
                adapterProperties.load(inputStream);
            } else
                throw new FileNotFoundException("Property file: '" + propFilename + "' not found in classpath.");
        } catch (IOException e) {
            System.out.println("Exception:" + e.getMessage());
        }

        return adapterProperties;
    }


    public static void main(String[] args) {
        MyBaseAdapter adapter = new MyBaseAdapter();
    }


    private SimpleEvent convertToSimpleEvent(String input) {
        // Input format: "{\"variable_type\":\"1002311\",\"value\":138.2346,\"variable_timestamp\":\"2014-02-14T07:01:24.133Z\"}";

        input = input.replace("\"", "");
        input = input.replace("{", "");
        input = input.replace("variable_type:", "");
        input = input.replace("value:", "");
        input = input.replace("variable_timestamp:", "");
        input = input.replace("}", "");

        String[] parts = input.split(",");
        System.out.println("parts[0] = " + parts[0]);
        System.out.println("parts[1] = " + parts[1]);
        System.out.println("parts[2] = " + parts[2]);

        long timestamp = new DateTime(parts[2]).getMillis();
        String sensorId = parts[0];

        // Define complex value
        ComplexValue value = new ComplexValue();
        value.setValue(parts[1]);
        value.setType(VariableType.LONG);

        // Define measurement
        Map<String, ComplexValue> measurement = new HashMap<String, ComplexValue>();
        measurement.put("raw.value", value);

        SimpleEvent event = new SimpleEvent();
        event.setTimestamp(timestamp);
        event.setSensorId(convertTagToSensorId(sensorId));
        event.setEventProperties(measurement);

        return event;
    }


    private String convertTagToSensorId(String tag) {
        String sensorId = "";

        if(tag.matches("1000693"))
            sensorId = "MHWirth.DDM.DrillingRPM";
        else if(tag.matches("1000700"))
            sensorId = "MHWirth.DDM.DrillingTorque";
        else if(tag.matches("1002311"))
            sensorId = "MHWirth.DDM.HookLoad";
        else if(tag.matches("1000695"))
            sensorId = "MHWirth.DDM.GearLubeOilTemp";
        else if(tag.matches("1000692"))
            sensorId = "MHWirth.DDM.GearBoxPressure";
        else if(tag.matches("1000696"))
            sensorId = "MHWirth.DDM.SwivelOilTemp";
        else if(tag.matches("1002123"))
            sensorId = "MHWirth.DrillBit.WeightOnBit";
        else if(tag.matches("1033619"))
            sensorId = "MHWirth.DrillBit.WeightOnBit";

        return sensorId;
    };


}