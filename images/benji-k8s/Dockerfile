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

RUN curl -o /usr/bin/kubectl -sSL https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && \
	chmod a+x /usr/bin/kubectl

COPY images/benji-k8s/bin/ $VENV_DIR/bin/
RUN chmod a+x $VENV_DIR/bin/*

ENTRYPOINT ["/bin/bash"]
CMD ["-c", "sleep 3650d"]
