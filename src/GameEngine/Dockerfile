FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["src/GameEngine/GameEngine.csproj", "src/GameEngine/"]
COPY ["src/RngService/RngService.csproj", "src/RngService/"]
COPY ["src/RngService/Protos/rng.proto", "src/RngService/Protos/"]
RUN dotnet restore "src/GameEngine/GameEngine.csproj"
COPY . .
WORKDIR "/src/src/GameEngine"
RUN dotnet publish "GameEngine.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "GameEngine.dll"]
