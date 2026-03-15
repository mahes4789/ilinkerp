# Deploy to Azure Web App (Linux Containers)

This project contains a FastAPI backend and a React frontend bundled into a multi-container Docker Compose setup.

## What was done to make this Azure-friendly
- The backend now reads persistent file paths (users, connection store, caches) from `APP_DATA_DIR` (default `/home`).
- The backend container listens on `$PORT` (default `8001`), which is required by Azure Web App for Containers.
- The frontend Nginx configuration proxies `/api/*` to the backend container (`http://backend:8001`).
- A `docker-compose.yml` is included so Azure Web App can run the full stack with a single public endpoint.

## Deploy using Azure Web App for Containers (Docker Compose)

### Pre-requisites
- An Azure subscription
- Azure CLI installed and logged in (`az login`)
- (Optional) Azure Container Registry if you want to build/push images ahead of time.

### Quick one-shot deployment using docker-compose
1. Create a resource group:
   ```sh
   az group create -n ilinkerp-rg -l eastus
   ```
2. Create an App Service plan (Linux):
   ```sh
   az appservice plan create -n ilinkerp-plan -g ilinkerp-rg --is-linux --sku B1
   ```
3. Create the Web App for Containers (multi-container):
   ```sh
   az webapp create -n ilinkerp-webapp -g ilinkerp-rg --plan ilinkerp-plan --multicontainer-config-type compose --multicontainer-config-file docker-compose.yml
   ```

### Setting app settings (env vars)
By default, the backend uses `/home` for persistent storage. Optionally you can set:
- `APP_DATA_DIR` to override the persistent directory.

Azure Web Apps running a multi-container setup also require the `WEBSITES_PORT` setting to know which container port to expose publicly. For this repo, the frontend listens on port 80.

To set these in the Web App:
```sh
az webapp config appsettings set -g ilinkerp-rg -n ilinkerp-webapp --settings APP_DATA_DIR=/home/data WEBSITES_PORT=80
```

### Build & push images (optional)
Azure Web App for Containers can build images for you based on the `docker-compose.yml`, but many teams prefer to build and push to a registry first (e.g., Azure Container Registry).

If you do that, update `docker-compose.yml` to reference `image:` tags instead of `build:`.

---

## Local development
- Run `docker-compose up --build`.
- Open http://localhost:3001 to access the app.

> Note: The frontend proxies `/api/*` to the backend using the service name `backend`, so you can keep the same API path in production.
