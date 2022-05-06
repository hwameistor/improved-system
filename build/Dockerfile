FROM centos:7

RUN yum install -y smartmontools lsscsi

COPY ./_build/improved-system /improved-system

ENTRYPOINT [ "/improved-system" ]
