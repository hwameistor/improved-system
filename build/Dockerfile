FROM centos:7

RUN yum install -y smartmontools lsscsi

COPY ./_build/reliable-helper-system /reliable-helper-system

ENTRYPOINT [ "/reliable-helper-system" ]
