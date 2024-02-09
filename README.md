# HikariCP

[![License: Apache v2](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/license/apache-2-0)
[![JitPack Badge](https://jitpack.io/v/Foulest/HikariCP.svg)](https://jitpack.io/#Foulest/HikariCP)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/58f57090224543b8a11e833059066852)](https://app.codacy.com/gh/Foulest/HikariCP/dashboard)

**HikariCP** is a lightweight, high-performance JDBC connection pool.

This is, of course, an updated fork of the original **[HikariCP](https://github.com/brettwooldridge/HikariCP)** project.

All credit for the original project goes to the
**[HikariCP](https://github.com/brettwooldridge/HikariCP/graphs/contributors)** team.

## Overview

I created this fork due to HikariCP dropping support for **Java 8** in version **4.0.3**.

This fork aims to fix any issues present in that version while maintaining Java 8 compatibility.

## Features

- All the features of the original HikariCP project *(version 4.0.3)*
- Re-coded the entire project to be more efficient and easier to maintain
- Updated outdated and [vulnerable dependencies](https://mvnrepository.com/artifact/com.zaxxer/HikariCP/4.0.3)
- Lombok has been implemented to reduce boilerplate code
- Migrated from using Maven to Gradle

## Getting Help

For support or queries, please open an issue in the [Issues section](https://github.com/Foulest/HikariCP/issues).
