FROM almalinux:8.5 AS build

ARG CEPH_CODENAME="pacific"
ARG CEPH_DISTRO="el8"

ENV VENV_DIR /benji

COPY images/benji/ceph.repo /etc/yum.repos.d/ceph.repo
RUN sed -i -e "s/{ceph-release}/$CEPH_CODENAME/" -e "s/{distro}/$CEPH_DISTRO/" /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	ulimit -n 1024 && \
	yum install -y tzdata epel-release && \
	yum update -y && \
	yum install -y git gcc make \
	python3-devel python3-pip python3-libs python3-setuptools \
	python3-rbd python3-rados

COPY . /benji-source/

RUN python3 -m venv --system-site-packages $VENV_DIR && \
	. $VENV_DIR/bin/activate && \
	pip install --upgrade pip setuptools && \
	pip install git+https://github.com/elemental-lf/libiscsi-python && \
	pip install '/benji-source/[compression,s3,b2,helpers]'

FROM almalinux:8.5 AS runtime

ARG VCS_REF
ARG VCS_URL
ARG VERSION 
ENV BENJI_VERSION_OVERRIDE=$VERSION
ARG BUILD_DATE

ENV VENV_DIR /benji

ENV PATH $VENV_DIR/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/root/bin

LABEL org.label-schema.schema-version="1.0" \
	org.label-schema.name="Benji" \
	org.label-schema.vendor="Benji's contributors" \
	org.label-schema.url="https://benji-backup.me/" \
	org.label-schema.license="LGPLv3" \
	org.label-schema.vcs-ref="$VCS_REF" \
	org.label-schema.vcs-url="$VCS_URL" \
	org.label-schema.build-date="$BUILD_DATE" \
	org.label-schema.version="$VERSION"

COPY --from=build /etc/yum.repos.d/ceph.repo /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	ulimit -n 1024 && \
	yum install -y tzdata epel-release && \
	yum update -y && \
	yum install -y python3 && \
	yum install -y ceph-base python3-rbd python3-rados && \
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
