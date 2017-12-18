FROM python:2.7-alpine
MAINTAINER Jacob Sanford <jacob.josh.sanford_at_gmail_com>

COPY src/ /usr/src/app/
WORKDIR /usr/src/app

RUN apk update && apk --update add p7zip && \
  rm -f /var/cache/apk/* && \
  pip install --no-cache-dir -r requirements.txt

CMD ["python", "/usr/src/app/parseHistory.py"]
