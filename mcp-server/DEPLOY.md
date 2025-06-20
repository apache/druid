# Deployment Guide

## NPX Deployment (Recommended)

### Quick Start
```bash
npx apache-druid-mcp
```

### With Custom Configuration
```bash
DRUID_URL=http://your-druid:8888 npx apache-druid-mcp
```

## Docker Deployment

### Prerequisites
- Docker installed and running
- Docker Compose (optional)

### Method 1: Docker Compose (Recommended)
```bash
# Clone/download the repository
git clone <repository-url>
cd mcp-server

# Start the server
docker-compose up --build

# Stop the server
docker-compose down
```

### Method 2: Docker Build & Run
```bash
# Build the image
docker build -t apache-druid-mcp .

# Run with default config
docker run -p 3000:3000 apache-druid-mcp

# Run with custom config
docker run -p 3000:3000 \
  -e DRUID_URL=http://your-druid:8888 \
  -e DRUID_USERNAME=admin \
  -e DRUID_PASSWORD=secret \
  apache-druid-mcp
```

### Method 3: Docker with Environment File
```bash
# Create .env file
cat > .env << EOF
DRUID_URL=http://localhost:8888
DRUID_USERNAME=
DRUID_PASSWORD=
DRUID_TIMEOUT=30000
NODE_ENV=production
EOF

# Run with environment file
docker run -p 3000:3000 --env-file .env apache-druid-mcp
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DRUID_URL` | `http://localhost:8888` | Druid broker/router URL |
| `DRUID_USERNAME` | - | Optional authentication username |
| `DRUID_PASSWORD` | - | Optional authentication password |
| `DRUID_TIMEOUT` | `30000` | Request timeout in milliseconds |

## Testing

### Test NPX Installation
```bash
# Install globally (optional)
npm install -g apache-druid-mcp

# Test help
apache-druid-mcp --help
```

### Test Docker
```bash
# Build
docker build -t apache-druid-mcp .

# Test help
docker run --rm apache-druid-mcp --help
```

## Troubleshooting

### NPX Issues
- Ensure Node.js 18+ is installed
- Check npm registry connectivity
- Clear npm cache: `npm cache clean --force`

### Docker Issues
- Ensure Docker daemon is running
- Check port 3000 is available
- Verify environment variables are set correctly 