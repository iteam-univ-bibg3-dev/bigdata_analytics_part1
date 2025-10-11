# es-init/Dockerfile
FROM alpine:3.19

# Install curl for HTTP requests
RUN apk add --no-cache curl

# Copy the init script and template file
COPY init_es.sh /init_es.sh
COPY nature_events_template.json /nature_events_template.json

# Make script executable
RUN chmod +x /init_es.sh

# Run script at container startup
ENTRYPOINT ["/init_es.sh"]
