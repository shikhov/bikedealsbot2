FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim

WORKDIR /usr/src/app
COPY src/ /usr/src/app
COPY requirements.txt ./
RUN uv pip install --no-cache --system -r requirements.txt
EXPOSE 8000
CMD [ "python", "app.py" ]