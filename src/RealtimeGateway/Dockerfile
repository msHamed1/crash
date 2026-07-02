FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["src/RealtimeGateway/RealtimeGateway.csproj", "src/RealtimeGateway/"]
RUN dotnet restore "src/RealtimeGateway/RealtimeGateway.csproj"
COPY . .
WORKDIR "/src/src/RealtimeGateway"
RUN dotnet publish "RealtimeGateway.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "RealtimeGateway.dll"]
