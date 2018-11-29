# Qlik2DataRobot

Qlik2DataRobot is an Analytic Connector for Qlik Sense, QlikView and Qlik Core.

## Configuration

- grpcHost: The allowed remote connection IP block (default: 0.0.0.0)
- grpcPort: The port upon which the grpc communication will take place (default: 50052)
- certificateFolder: The certificate folder for secure communication with Qlik Engine

## Deployment
The connector has been written in c#, there is a version for .NET Framework v4.6.1 primarily for easy installation on Windows (an installation package is included) and another using dotnet core 2.1. This makes the code portable across operating systems. By default, the windows version is compiled and provided in the GitHub releases page. DotNet Core can be compiled manually or used with the accompanying Docker image.

Docker files are included to allow for build and deployment as a docker image using official Microsoft dotnet base images.
