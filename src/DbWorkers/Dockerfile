FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/DbWorkers/DbWorkers.csproj", "src/DbWorkers/"]
COPY ["src/Crash.Domain/Crash.Domain.csproj", "src/Crash.Domain/"]
COPY ["src/Crash.Persistence/Crash.Persistence.csproj", "src/Crash.Persistence/"]
RUN dotnet restore "src/DbWorkers/DbWorkers.csproj"
COPY . .
WORKDIR "/src/src/DbWorkers"
RUN dotnet build "./DbWorkers.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./DbWorkers.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DbWorkers.dll"]
