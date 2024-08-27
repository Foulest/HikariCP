# HikariCP

[![License: Apache v2](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/license/apache-2-0)
[![CodeQL Badge](https://github.com/Foulest/HikariCP/actions/workflows/codeql.yml/badge.svg)](https://github.com/Foulest/HikariCP/actions/workflows/codeql.yml)
[![JitPack Badge](https://jitpack.io/v/Foulest/HikariCP.svg)](https://jitpack.io/#Foulest/HikariCP)
[![Downloads](https://img.shields.io/github/downloads/Foulest/HikariCP/total.svg)](https://github.com/Foulest/HikariCP/releases)

**HikariCP** is a lightweight, high-performance JDBC connection pool.

> **Note:** This is an updated fork of the original **[HikariCP](https://github.com/brettwooldridge/HikariCP)** project.
> 
> All credit for the original project goes to the
**[HikariCP](https://github.com/brettwooldridge/HikariCP/graphs/contributors)** team.

## Changes Made

- Includes all features of the original HikariCP project *(version 4.0.3)*
- Re-coded the entire project for improved efficiency and maintainability
- Maintained **Java 8** compatibility that was dropped in the original project
- Updated outdated and [vulnerable dependencies](https://mvnrepository.com/artifact/com.zaxxer/HikariCP/4.0.3)
- Added JetBrains annotations to improve code quality
- Implemented Lombok to reduce boilerplate code
- Migrated from using Maven to Gradle

## Compiling

1. Clone the repository.
2. Open a command prompt/terminal to the repository directory.
3. Run `gradlew shadowJar` on Windows, or `./gradlew shadowJar` on macOS or Linux.
4. The built `HikariCP-X.X.X.jar` file will be in the `build/libs` folder.

## Getting Help

For support or queries, please open an issue in the [Issues section](https://github.com/Foulest/HikariCP/issues).
