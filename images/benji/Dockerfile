FROM centos:7 AS build

# For the Debian repository
ARG CEPH_CODENAME="mimic"
ARG CEPH_DISTRO="el7"
# For the Git checkout
ARG CEPH_VERSION="13.2.2"

# If you change VENV_PATH you also need to change the path to activate in scripts/backup.sh.
ENV VENV_PATH /benji

COPY images/common/ceph.repo /etc/yum.repos.d/ceph.repo
RUN sed -i -e "s/{ceph-release}/$CEPH_CODENAME/" -e "s/{distro}/$CEPH_DISTRO/" /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	yum install -y https://centos7.iuscommunity.org/ius-release.rpm && \
	yum update -y && \
	yum install -y git gcc && \
	yum install -y python36u-devel python36u-pip python36u-libs python36u-setuptools && \
	yum install -y librados-devel librbd-devel

RUN python3.6 -m venv $VENV_PATH && \
	ln -s /usr/bin/python3.6-config $VENV_PATH/bin/python-config && \
	ln -s /usr/bin/python3.6m-x86_64-config $VENV_PATH/bin/python3.6m-x86_64-config && \
	. $VENV_PATH/bin/activate && \
	pip install --upgrade 'pip==10.0.*' && \
	sed -i "s|self.run_command(\['clone', '-q', url, dest\])|self.run_command(['clone', '--depth=1', '--branch', rev if rev else 'master', '-q', url, dest])|" $(python -c 'import site; print(site.getsitepackages()[0])')/pip/_internal/vcs/git.py

RUN . $VENV_PATH/bin/activate && \
	pip install cython && \
	pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rados" && \
	pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rbd"

ADD . /benji-source/
RUN . $VENV_PATH/bin/activate && \
	pip install '/benji-source/[compression,s3,readcache,b2]'

FROM centos:7 AS runtime

ARG VCS_REF
ARG VCS_URL
ARG VERSION 
ARG BUILD_DATE

LABEL org.label-schema.schema-version="1.0" \
      org.label-schema.vcs-ref="$VCS_REF" \
      org.label-schema.vcs-url="$VCS_URL" \
      org.label-schema.build-date="$BUILD_DATE" \
      org.label-schema.version="$VERSION" \
      org.label-schema.url="https://benji-backup.me/"

ENV SCRIPTS_PATH /scripts

COPY --from=build /etc/yum.repos.d/ceph.repo /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	yum install -y https://centos7.iuscommunity.org/ius-release.rpm && \
	yum install -y epel-release && \
	yum update -y && \
	yum install -y python36u-devel python36u-pip python36u-libs python36u-setuptools && \
	yum install -y librados2 librbd1 ceph-base && \
	yum install -y bash-completion joe jq && \
	yum clean all

COPY --from=build $VENV_PATH/ $VENV_PATH/
COPY scripts/ $SCRIPTS_PATH/
COPY etc/benji-minimal.yaml /etc/benji/benji.yaml
COPY images/common/benji-wrapper /usr/bin/benji
COPY images/common/bashrc /root/.bashrc

RUN curl -o /usr/bin/kubectl -sSL https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && \
	chmod a+x /usr/bin/kubectl /usr/bin/benji

ENTRYPOINT ["/bin/bash"]
CMD ["-i"]
