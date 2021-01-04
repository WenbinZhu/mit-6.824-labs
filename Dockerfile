FROM ubuntu:18.04


RUN apt update -y
RUN apt install -y man vim curl wget gcc gdb lsof net-tools build-essential make sudo software-properties-common
RUN apt update -y

RUN sudo add-apt-repository ppa:longsleep/golang-backports
RUN sudo apt update -y
RUN sudo apt install -y golang-go

# RUN rm -rf /var/lib/apt/lists/*

RUN mkdir mit-6.824-labs

CMD ["/bin/bash"]
