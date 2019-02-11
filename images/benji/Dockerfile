FROM centos:7 AS build

# For the Debian repository
ARG CEPH_CODENAME="mimic"
ARG CEPH_DISTRO="el7"
# For the Git checkout
ARG CEPH_VERSION="13.2.4"

ENV VENV_DIR /benji

COPY images/benji/ceph.repo /etc/yum.repos.d/ceph.repo
RUN sed -i -e "s/{ceph-release}/$CEPH_CODENAME/" -e "s/{distro}/$CEPH_DISTRO/" /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	yum install -y deltarpm tzdata epel-release https://centos7.iuscommunity.org/ius-release.rpm && \
	yum update -y && \
	yum install -y git gcc \
                   python36u-devel python36u-pip python36u-libs python36u-setuptools \
                   librados-devel librbd-devel

RUN python3.6 -m venv $VENV_DIR && \
	ln -s /usr/bin/python3.6-config $VENV_DIR/bin/python-config && \
	ln -s /usr/bin/python3.6m-x86_64-config $VENV_DIR/bin/python3.6m-x86_64-config && \
	. $VENV_DIR/bin/activate && \
	pip install --upgrade 'pip==10.0.*' && \
	sed -i "s|self.run_command(\['clone', '-q', url, dest\])|self.run_command(['clone', '--depth=1', '--branch', rev if rev else 'master', '-q', url, dest])|" $(python -c 'import site; print(site.getsitepackages()[0])')/pip/_internal/vcs/git.py

RUN . $VENV_DIR/bin/activate && \
	pip install cython && \
	pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rados" && \
	pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rbd"

ADD . /benji-source/
RUN . $VENV_DIR/bin/activate && \
	pip install '/benji-source/[compression,s3,readcache,b2]'

FROM centos:7 AS runtime

ARG VCS_REF
ARG VCS_URL
ARG VERSION 
ENV IMAGE_VERSION=$VERSION
ARG BUILD_DATE

ENV VENV_DIR /benji

ENV PATH $VENV_DIR/scripts:$VENV_DIR/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/root/bin

LABEL org.label-schema.schema-version="1.0" \
      org.label-schema.vcs-ref="$VCS_REF" \
      org.label-schema.vcs-url="$VCS_URL" \
      org.label-schema.build-date="$BUILD_DATE" \
      org.label-schema.version="$VERSION" \
      org.label-schema.url="https://benji-backup.me/"

COPY --from=build /etc/yum.repos.d/ceph.repo /etc/yum.repos.d/ceph.repo

RUN rpm --import 'https://download.ceph.com/keys/release.asc' && \
	yum install -y deltarpm tzdata epel-release https://centos7.iuscommunity.org/ius-release.rpm && \
	yum update -y && \
	yum install -y python36u-devel python36u-pip python36u-libs python36u-setuptools && \
	yum install -y librados2 librbd1 ceph-base && \
	yum install -y bash-completion joe jq && \
	yum clean all

RUN mkdir /etc/benji && \
    ln -s $VENV_DIR/etc/benji.yaml /etc/benji/benji.yaml && \
    echo "PATH=$PATH" >>/etc/environment

COPY --from=build $VENV_DIR/ $VENV_DIR/
COPY scripts/ $VENV_DIR/scripts/
COPY etc/benji-minimal.yaml $VENV_DIR/etc/benji.yaml
COPY images/benji/bashrc /root/.bashrc

RUN chmod -R a+x $VENV_DIR/scripts/

WORKDIR $VENV_DIR

ENTRYPOINT ["/bin/bash"]
CMD ["-il"]
