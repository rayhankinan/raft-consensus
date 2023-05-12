FROM python:3.11.3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./server ./

CMD [ "python", "-u", "./main.py" ]

EXPOSE 8080