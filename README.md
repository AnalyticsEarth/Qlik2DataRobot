# Qlik2DataRobot

Qlik2DataRobot is an Analytic Connector for Qlik Sense, QlikView and Qlik Core.

For more information on this project and integrating Qlik with DataRobot visit [Qlik Community](http://bit.ly/QlikDataRobot).

A companion client extension is available [here](https://github.com/AnalyticsEarth/Qlik2DataRobot-Ext).

## Download
The source code is not suitable for installing directly into Qlik Sense, please download the latest release from the releases page on this GitHub repository.

## Installation
For installation instructions, visit the Installation Guide which is part of the Qlik2DataRobot repository:
https://github.com/AnalyticsEarth/Qlik2DataRobot/tree/master/docs

## Issues
Any issues with the extension should be recorded as such in this GitHub repository.

## Support
This extension is provided as Open Source and not covered as part of your Qlik maintenance. Support is provided by the community.

## Configuration

- grpcHost: The allowed remote connection IP block (default: 0.0.0.0)
- grpcPort: The port upon which the grpc communication will take place (default: 50052)
- certificateFolder: The certificate folder for secure communication with Qlik Engine

## Deployment
The connector has been written in c#, there is a version for .NET Framework v4.6.1 primarily for easy installation on Windows (an installation package is included) and another using dotnet core 2.1. This makes the code portable across operating systems. By default, the windows version is compiled and provided in the GitHub releases page. DotNet Core can be compiled manually or used with the accompanying Docker image.

Docker files are included to allow for build and deployment as a docker image using official Microsoft dotnet base images.
