# Use an official Python runtime as a parent image
FROM python:3.8-alpine

# Set the working directory to /app
WORKDIR /app

# Environment variable to get settings file
ENV SETTINGS_FILE /var/lib/cassandra-gateway/private/settings/cassandra-gateway.ini

# Copy requirements file
COPY src/requirements.txt .

# Install build dependencies
#  install any needed packages specified in requirements.txt
#  remove directory if (test or tests) or remove files if (*.pyo or *.pyc)
#  then clean build dependencies
RUN apk --no-cache add --virtual .build-dependencies build-base \
    && pip install --no-cache -r requirements.txt \
    && find /usr/local \( -type d -a -name test -o -name tests \) \
        -o \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
        -exec rm -rf '{}' + \
    && apk del .build-dependencies

# Copy the src directory content into the container /app/ folder
COPY src/ ./

# compile all source code
#  python -m compileall
#   -b to place compiled file next to source ones
#   -q to quiet output (show only error)
#   -f force rebuild
#   -j 0 to use all cores
#   ./ is the project path to compile
# then remove source code files
RUN python -m compileall -b -q -f -j 0 ./ \
    && find . -name "*.py" -type f -delete

# add small health check on this webserver
HEALTHCHECK --interval=20s --timeout=3s --start-period=1s --retries=2 \
    CMD python tests/healthcheck.pyc

# add labels to this image
LABEL version="dev" \
    commit="" \
    maintainer="emeric254"

# Run app.py when the container launches
CMD ["python", "app.pyc"]

