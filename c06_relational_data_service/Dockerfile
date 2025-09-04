# Base AWS Lambda Python 3.10 image
FROM public.ecr.aws/lambda/python:3.10

# Install dependencies needed for uv installer
RUN yum install -y tar gzip xz && yum clean all

# Install uv (standalone binary)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv

# Copy application code into Lambda task root
COPY . ${LAMBDA_TASK_ROOT}

# use lockfile to install dependencies
RUN uv sync

# Expose Lambda runtime port
EXPOSE 8080

# Lambda entrypoint (module.function)
CMD [ "parquet_to_rds.lambda_handler" ]
