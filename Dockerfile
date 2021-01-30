FROM python:3.8.6-buster
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /opt/ws_tickers
WORKDIR /opt/ws_tickers

EXPOSE 8080/tcp

CMD [ "python", "main.py" ]