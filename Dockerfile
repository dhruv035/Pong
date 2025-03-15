FROM node:18-alpine

WORKDIR /app

# Install dependencies
COPY package.json pnpm-lock.yaml ./
RUN npm install -g pnpm && pnpm install --frozen-lockfile

# Copy app source
COPY . .

# Build the TypeScript code
RUN pnpm build

# Set environment variables
ENV NODE_ENV=production

# Run the app
CMD ["node", "dist/index.js"] 