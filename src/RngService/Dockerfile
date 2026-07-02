FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["src/RngService/RngService.csproj", "src/RngService/"]
RUN dotnet restore "src/RngService/RngService.csproj"
COPY . .
WORKDIR "/src/src/RngService"
RUN dotnet publish "RngService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "RngService.dll"]
