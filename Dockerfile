FROM mageai/mageai:0.9.79

ARG PROJECT_NAME=pipelines
ARG MAGE_CODE_PATH=/home/src
ARG USER_CODE_PATH=${MAGE_CODE_PATH}/${PROJECT_NAME}

WORKDIR ${MAGE_CODE_PATH}

# Copy project files
COPY . .

ENV USER_CODE_PATH=${USER_CODE_PATH}

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv 

# Install custom lib
RUN uv sync --frozen --no-dev --no-install-project
RUN source .venv/bin/activate

ENV PYTHONPATH="${PYTHONPATH}:${MAGE_CODE_PATH}"

RUN ls -a

CMD ["/bin/sh", "-c", "/app/run_app.sh"]
