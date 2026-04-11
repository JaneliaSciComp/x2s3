#!/bin/bash
set -e

# Initialize pixi environment and install package dependencies
pixi install
pixi run dev-install

# Copy config template if config doesn't exist
if [ ! -f "config.yaml" ]; then
    cp config.template.yaml config.yaml
    echo "Created config.yaml from template - please configure your targets"
fi

# Create var directory for credentials
mkdir -p var

echo "Dev container setup complete!"
echo "Run 'pixi run dev-launch' to start the development server"
