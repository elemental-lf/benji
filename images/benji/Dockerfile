FROM centos:7 AS build

ARG CEPH_CODENAME="nautilus"
ARG CEPH_DISTRO="el7"

ENV VENV_DIR /benji

COPY images/benji/ceph.repo /etc/yum.repos.d/ceph.repo
RUN sed -i -e "s/{ceph-release}/$CEPH_CODENAME/" -e "s/{distro}/$CEPH_DISTRO/" /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	yum install -y tzdata epel-release && \
	yum update -y && \
	yum install -y git gcc make \
		python36-devel python36-pip python36-libs python36-setuptools \
		python36-rbd python36-rados

ADD . /benji-source/

RUN python3.6 -m venv --system-site-packages $VENV_DIR && \
	. $VENV_DIR/bin/activate && \
	pip install git+https://github.com/elemental-lf/libiscsi-python && \
	pip install '/benji-source/[compression,s3,b2,helpers]'

FROM centos:7 AS runtime

ARG VCS_REF
ARG VCS_URL
ARG VERSION 
ENV IMAGE_VERSION=$VERSION
ARG BUILD_DATE

ENV VENV_DIR /benji

ENV PATH $VENV_DIR/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/root/bin

LABEL org.label-schema.schema-version="1.0" \
      org.label-schema.vcs-ref="$VCS_REF" \
      org.label-schema.vcs-url="$VCS_URL" \
      org.label-schema.build-date="$BUILD_DATE" \
      org.label-schema.version="$VERSION" \
      org.label-schema.url="https://benji-backup.me/"

COPY --from=build /etc/yum.repos.d/ceph.repo /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	yum install -y tzdata epel-release && \
	yum update -y && \
	yum install -y python36 && \
	yum install -y ceph-base python36-rbd python36-rados && \
	yum install -y bash-completion joe jq && \
	yum clean all

RUN mkdir /etc/benji && \
    ln -s $VENV_DIR/etc/benji.yaml /etc/benji/benji.yaml && \
    echo "PATH=$PATH" >>/etc/environment

COPY --from=build $VENV_DIR/ $VENV_DIR/
COPY etc/benji-minimal.yaml $VENV_DIR/etc/benji.yaml
COPY images/benji/bashrc /root/.bashrc

WORKDIR $VENV_DIR

ENTRYPOINT ["/bin/bash"]
CMD ["-il"]
