# The OxiDB client/EF packages are referenced by project (the repo is private,
# so there is no nuget.org to pull them from) — which is why the build context
# is the repo root, exactly like the ColdChain demo.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY dotnet/ dotnet/
COPY bugtracker/ bugtracker/
RUN dotnet publish bugtracker/BugTracker.Api -c Release -o /out/api

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS api
WORKDIR /app
COPY --from=build /out/api .
ENV ASPNETCORE_URLS=http://0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "BugTracker.Api.dll"]
