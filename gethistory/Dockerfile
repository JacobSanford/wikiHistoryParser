FROM python:2.7-alpine
MAINTAINER Jacob Sanford <jacob.josh.sanford_at_gmail_com>

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY getHistoryDumps.py ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "/usr/src/app/getHistoryDumps.py"]
