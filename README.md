# Graylog2 MQTT Input Plugin
[![Build Status](https://travis-ci.org/Graylog2/graylog2-input-mqtt.svg?branch=master)](https://travis-ci.org/Graylog2/graylog2-input-mqtt)

This is an input plugin that allows you to subscribe to an [MQTT](http://mqtt.org) broker and index all published messages.
It _requires_ a recent Graylog2 server (0.21 or higher)

Getting started for users
-------------------------

* Clone this repository
* run `mvn package` to build a jar file.
* Copy generated jar file in target directory to your Graylog2 server plugin directory
* Restart Graylog2 server
* Create a new MQTT Input in the Graylog2 web interface
