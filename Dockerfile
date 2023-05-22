FROM ubuntu:22.04

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3-pip
RUN apt-get install iproute2 iputils-ping -y
RUN pip install --no-cache-dir -r requirements.txt

COPY ./server ./

CMD [ "tc", "qdisc", "add", "dev", "lo", "root", "netem", "delay", "100ms", "10ms", "25%", "loss", "10%", "25%", "corrupt", "10%", "25%", "duplicate", "10%", "25%" && "python3", "-u", "./main.py" ]

EXPOSE 8080