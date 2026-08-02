# Builds all three ColdChain apps in one pass. The OxiDB client packages are
# referenced by project (the repo is private, so there is no nuget.org to pull
# them from), which is why the build context is the repo root.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY dotnet/ dotnet/
COPY coldchain/ coldchain/
RUN dotnet publish coldchain/ColdChain.Api       -c Release -o /out/api \
 && dotnet publish coldchain/ColdChain.Ingest    -c Release -o /out/ingest \
 && dotnet publish coldchain/ColdChain.Simulator -c Release -o /out/simulator

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS api
WORKDIR /app
COPY --from=build /out/api .
ENV ASPNETCORE_URLS=http://0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "ColdChain.Api.dll"]

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS ingest
WORKDIR /app
COPY --from=build /out/ingest .
ENTRYPOINT ["dotnet", "ColdChain.Ingest.dll"]

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS simulator
WORKDIR /app
COPY --from=build /out/simulator .
ENTRYPOINT ["dotnet", "ColdChain.Simulator.dll"]
