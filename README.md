# Graylog MQTT Input Plugin
[![Build Status](https://travis-ci.org/Graylog2/graylog2-input-mqtt.svg?branch=v1.1.0)](https://travis-ci.org/Graylog2/graylog2-input-mqtt)

This is an input plugin that allows you to subscribe to an [MQTT](http://mqtt.org) broker and index all published messages.
It _requires_ a recent Graylog server (1.0.0 or higher)

Getting started for users
-------------------------

* Clone this repository
* run `mvn package` to build a jar file.
* Copy generated jar file in target directory to your Graylog server plugin directory
* Restart Graylog server
* Create a new MQTT Input in the Graylog web interface
