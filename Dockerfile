FROM microsoft/dotnet:2.1-sdk as builder  
 
RUN mkdir -p /root/src/app/qlik2datarobot
WORKDIR /root/src/app/qlik2datarobot
 
COPY Qlik2DataRobot/Qlik2DataRobot.csproj Qlik2DataRobot/Qlik2DataRobot.csproj
RUN dotnet restore Qlik2DataRobot/Qlik2DataRobot.csproj 

COPY . .
RUN dotnet publish -c release -o published 

FROM microsoft/dotnet:2.1-runtime

WORKDIR /root/  
COPY --from=builder /root/src/app/qlik2datarobot/Qlik2DataRobot/published .

EXPOSE 50052/tcp
EXPOSE 19345/tcp
CMD ["dotnet","./Qlik2DataRobot.dll"]