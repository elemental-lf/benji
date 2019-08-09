FROM python:3.7 AS build

ADD . /benji-source/
RUN pip install '/benji-source/[helpers,k8s-operator]'

FROM python:3.7 AS runtime

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

COPY --from=build /usr/local/ /usr/local/

CMD kopf run -mbenji.k8s_operator
