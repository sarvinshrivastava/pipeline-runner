FROM oven/bun:1-alpine

# docker-cli: the runner spawns a sibling container per pipeline run via the
# mounted host socket. git: clone/commit/push happen service-side so a
# pipeline's container never sees the git token.
RUN apk add --no-cache docker-cli git ca-certificates tzdata

WORKDIR /app

COPY package.json bun.lock* ./
RUN bun install --production

COPY src ./src
COPY public ./public

ENV PORT=3000
EXPOSE 3000

CMD ["bun", "run", "src/index.ts"]
