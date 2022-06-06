package com.tericcabrel;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.tericcabrel.fingerprint.FingerPrint;
import com.tericcabrel.fingerprint.FingerprintScanner;
import com.tericcabrel.services.OsirisCardService;
import com.tericcabrel.utils.CardHelper;
import com.tericcabrel.utils.Helpers;
import com.tericcabrel.utils.Messaging;

import javax.smartcardio.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

public class Main {
    private static Channel channel;
    private static String picturePath = "D:\\Card\\Data";
    private static String templatePath = "D:\\Card\\Data\\template";
    private static String tempUid = null;

    public static void main(String[] args) {
        // Connection to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");

        try {
            Connection connection = factory.newConnection();
            System.out.println(" Connected successfully to RabbitMQ Server");

            channel = connection.createChannel();
        }catch (TimeoutException | IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        new Timer().scheduleAtFixedRate(new TimerTask(){
            @Override
            public void run() {
                try {
                    Card newCard = CardHelper.getCard();

                    if (OsirisCardService.getCard() == null && newCard != null) {
                        System.out.println("Carte inserer avec succes");

                        OsirisCardService.setCard(newCard);

                        String message = OsirisCardService.selectApplet();

                        Messaging.sendToQueue(channel, Messaging.Q_APPLET_SELECTED_RESPONSE, message);
                    } else if (OsirisCardService.getCard() != null && newCard == null) {
                        System.out.println("Carte retirer avec succes");

                        OsirisCardService.setCard(null);

                        Messaging.sendToQueue(channel, Messaging.Q_CARD_REMOVED_RESPONSE, OsirisCardService.SW_CARD_REMOVED);
                    }
                } catch (Exception e) {
                    // e.printStackTrace();
                    // TODO Stop the timer and notify that the service is down
                }
            }
        },0,1000);

        try {
            channel.queueDeclare(Messaging.Q_AUTHENTICATE_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");
                String response = OsirisCardService.authenticate(message);
                Messaging.sendToQueue(channel, Messaging.Q_AUTHENTICATE_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_AUTHENTICATE_REQUEST, true, deliverCallback, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_GET_DATA_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String response = OsirisCardService.getData();
                Messaging.sendToQueue(channel, Messaging.Q_GET_DATA_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_GET_DATA_REQUEST, true, deliverCallback1, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_SET_DATA_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");
                String response = OsirisCardService.setData(message);
                Messaging.sendToQueue(channel, Messaging.Q_SET_DATA_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_SET_DATA_REQUEST, true, deliverCallback2, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_SET_NAME_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback3 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");
                String response = OsirisCardService.setName(message);
                Messaging.sendToQueue(channel, Messaging.Q_SET_NAME_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_SET_NAME_REQUEST, true, deliverCallback3, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_SET_BIRTH_DATE_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback4 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");
                String response = OsirisCardService.setBirthDate(message);
                Messaging.sendToQueue(channel, Messaging.Q_SET_BIRTH_DATE_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_SET_BIRTH_DATE_REQUEST, true, deliverCallback4, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_RESET_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback5 = (consumerTag, delivery) -> {
                String response = OsirisCardService.resetData();
                Messaging.sendToQueue(channel, Messaging.Q_RESET_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_RESET_REQUEST, true, deliverCallback5, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_UNBLOCK_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback6 = (consumerTag, delivery) -> {
                String response = OsirisCardService.unblock();
                Messaging.sendToQueue(channel, Messaging.Q_UNBLOCK_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_UNBLOCK_REQUEST, true, deliverCallback6, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_DISCONNECT_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback7 = (consumerTag, delivery) -> {
                OsirisCardService.disconnect();
                Messaging.sendToQueue(channel, Messaging.Q_DISCONNECT_RESPONSE, "OK");
            };
            channel.basicConsume(Messaging.Q_DISCONNECT_REQUEST, true, deliverCallback7, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_ENROLL_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback8 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8); // Received the uid
                System.out.println(" [x] Received '" + message + "'"); // contains user's uid
                tempUid = message;

                Messaging.sendToQueue(channel, Messaging.Q_CAPTURE_REQUEST, message);
            };
            channel.basicConsume(Messaging.Q_ENROLL_REQUEST, true, deliverCallback8, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_CAPTURE_RESPONSE, false, false, false, null);
            DeliverCallback deliverCallback10 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8); // Received the uid
                System.out.println(" [x] Received '" + message + "'"); // contains user's uid

                String response = "12500";
                String templateFile = templatePath + "\\" + tempUid + ".tmpl";

                if (message.equals("SUCCESS")) {
                    // Write fingerPrint template in the card
                    OsirisCardService.setFingerprint(Helpers.fileToByteArray(templateFile));

                    // Upload the fingerprint to the server
                    String res = Helpers.uploadFingerprint(
                            tempUid,
                            templateFile,
                            picturePath + "\\" + tempUid + ".bmp"
                    );

                    if (!res.equals("RES200")) {
                        response = "12400";
                    }

                    // TODO delete files
                    tempUid = null;
                } else {
                    response = "12000";
                }

                Messaging.sendToQueue(channel, Messaging.Q_ENROLL_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_CAPTURE_RESPONSE, true, deliverCallback10, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_VERIFY_USER_REQUEST, false, false, false, null);
            DeliverCallback deliverCallback11 = (consumerTag, delivery) -> {
                String info = OsirisCardService.getData();
                System.out.println(info);

                String[] array = info.split(OsirisCardService.DATA_DELIMITER);

                System.out.println(array.length);
                if (array.length != 4) {
                    Messaging.sendToQueue(channel, Messaging.Q_VERIFY_USER_RESPONSE, info);
                    return;
                }

                // String templateFile = templatePath + "\\" + array[0] + ".tmpl";
                // System.out.println(templateFile);

                byte[] storedFingerprint = OsirisCardService.getFingerpint(Integer.parseInt(array[array.length - 1]));

                Messaging.sendToQueue(channel, Messaging.Q_PERFORM_VERIFY_REQUEST, storedFingerprint);
            };
            channel.basicConsume(Messaging.Q_VERIFY_USER_REQUEST, true, deliverCallback11, consumerTag -> { });

            channel.queueDeclare(Messaging.Q_PERFORM_VERIFY_RESPONSE, false, false, false, null);
            DeliverCallback deliverCallback12 = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8); // Received the uid
                System.out.println(" [x] Received '" + message + "'"); // contains user's uid
                String response = message;

                if (message.equals("SUCCESS")) {
                    response = OsirisCardService.getData();
                }
                Messaging.sendToQueue(channel, Messaging.Q_VERIFY_USER_RESPONSE, response);
            };
            channel.basicConsume(Messaging.Q_PERFORM_VERIFY_RESPONSE, true, deliverCallback12, consumerTag -> { });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}