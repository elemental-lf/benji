ARG BASE_TAG
FROM elementalnet/benji:${BASE_TAG}

ARG VCS_REF
ARG VCS_URL
ARG VERSION 
ENV IMAGE_VERSION=$VERSION
ARG BUILD_DATE

LABEL org.label-schema.schema-version="1.0" \
      org.label-schema.vcs-ref="$VCS_REF" \
      org.label-schema.vcs-url="$VCS_URL" \
      org.label-schema.build-date="$BUILD_DATE" \
      org.label-schema.version="$VERSION" \
      org.label-schema.url="https://benji-backup.me/"

RUN yum install -y cronie supervisor && \
    yum clean all

RUN curl -o /usr/bin/kubectl -sSL https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && \
	chmod a+x /usr/bin/kubectl

COPY images/benji-k8s/supervisord.conf /etc/supervisord.conf
COPY images/benji-k8s/crontab $VENV_DIR/etc/crontab
COPY images/benji-k8s/scripts/ $VENV_DIR/scripts/
COPY images/benji-k8s/bin/ $VENV_DIR/bin/

RUN chmod -R a+x $VENV_DIR/scripts/ && \
    chmod 644 $VENV_DIR/etc/crontab && \
    rm -f /etc/crontab /etc/cron.d/* && \
    ln -s $VENV_DIR/etc/crontab /etc/crontab  && \
    sed -i '/pam_systemd.so/d' /etc/pam.d/password-auth

ENTRYPOINT ["/usr/bin/supervisord"]
CMD ["-c", "/etc/supervisord.conf", "-u", "root", "-n"]
