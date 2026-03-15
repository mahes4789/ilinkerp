#!/bin/bash
# Azure App Service startup script for ilinkERP Fabric backend.
# Sets PYTHONPATH explicitly so uvicorn and all packages are found
# without depending on the PYTHONPATH app setting being loaded first.

set -e

# Packages pre-installed by CI into .python_packages/
export PYTHONPATH="/home/site/wwwroot/.python_packages/lib/site-packages"

cd /home/site/wwwroot

exec python3 -m uvicorn main:app \
  --host 0.0.0.0 \
  --port 8001 \
  --log-level info
