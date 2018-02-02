MQTT Plugin for Graylog
=======================

[![Github Downloads](https://img.shields.io/github/downloads/graylog-labs/graylog-plugin-mqtt/total.svg)](https://github.com/graylog-labs/graylog-plugin-mqtt/releases)
[![GitHub Release](https://img.shields.io/github/release/graylog-labs/graylog-plugin-mqtt.svg)](https://github.com/graylog-labs/graylog-plugin-mqtt/releases)
[![Build Status](https://travis-ci.org/graylog-labs/graylog-plugin-mqtt.svg)](https://travis-ci.org/graylog-labs/graylog-plugin-mqtt)

This is an input plugin that allows you to subscribe to an [MQTT](http://mqtt.org) broker and index all published messages.

**Required Graylog version:** 2.0.0 and later

## Installation

[Download the plugin](https://github.com/graylog-labs/graylog-plugin-mqtt/releases)
and place the `.jar` file in your Graylog plugin directory. The plugin directory
is the `plugins/` folder relative from your `graylog-server` directory by default
and can be configured in your `graylog.conf` file.

Restart `graylog-server` and you are done.

## Build

This project is using Maven 3 and requires Java 8 or higher.

You can build a plugin (JAR) with `mvn package`.

DEB and RPM packages can be build with `mvn jdeb:jdeb` and `mvn rpm:rpm` respectively.

## Plugin Release

We are using the maven release plugin:

```
$ mvn release:prepare
[...]
$ mvn release:perform
```

This sets the version numbers, creates a tag and pushes to GitHub. Travis CI will build the release artifacts and upload to GitHub automatically.
