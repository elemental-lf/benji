ARG BASE_TAG
ARG BASE_IMAGE
FROM ${BASE_IMAGE}:${BASE_TAG}

ARG VCS_REF
ARG VCS_URL
ARG VERSION 
ENV BENJI_VERSION_OVERRIDE=$VERSION
ARG BUILD_DATE

LABEL org.label-schema.schema-version="1.0" \
      org.label-schema.vcs-ref="$VCS_REF" \
      org.label-schema.vcs-url="$VCS_URL" \
      org.label-schema.build-date="$BUILD_DATE" \
      org.label-schema.version="$VERSION" \
      org.label-schema.url="https://benji-backup.me/"

RUN curl -o /usr/bin/kubectl -sSL https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && \
	chmod a+x /usr/bin/kubectl

COPY images/benji-k8s/k8s-tools /k8s-tools-source
RUN . $VENV_DIR/bin/activate && \
    pip install /k8s-tools-source && \
    rm -rf /k8s-tools-source

ENTRYPOINT ["/bin/bash"]
CMD ["-c", "sleep 3650d"]
